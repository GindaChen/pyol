import os
import subprocess
from enum import Enum
from os.path import abspath, join

import requests

from pyol.config import Config, Path_t, Limits, Features, Trace, Storage
from pyol.utils import logger
from pyol.workload import Workload


class Status(Enum):
    NOENV = 0  # worker environment has not initialized yet
    STOP = 1  # worker not running.
    RUNNING = 2  # worker running.
    KILLING = 3  # worker killing itself.


class Worker(Config):
    STATUS = 'status'
    DEBUG = 'debug'
    RUN = 'run'

    def __init__(
            self, executable_path=None,
            # Config native parameter
            use_tmpfs: bool = None,

            # _Config parameters
            worker_dir: Path_t = None,
            worker_port: str = None, sandbox: str = None,
            server_mode: str = None,
            registry_cache_ms: int = None,
            pip_mirror: str = None, mem_pool_mb: int = None, import_cache_tree: str = None,
            sandbox_config: dict = None, docker_runtime: str = None,
            limits: Limits = None, features: Features = None, trace: Trace = None, storage: Storage = None,

            # Other settings
            schema=None, hostname=None,
    ):
        """The work directory will be a separated entity that eventually injected into config."""

        # TODO: Inject the worker_dir to the setting of config.
        worker_dir = abspath(worker_dir) or abspath(join(os.getcwd(), "default-ol"))
        self.worker_dir = worker_dir
        logger.info(f'Use worker_dir={worker_dir}')

        super(Worker, self).__init__(
            use_tmpfs=use_tmpfs,
            worker_dir=worker_dir, worker_port=worker_port,
            sandbox=sandbox, server_mode=server_mode,
            registry_cache_ms=registry_cache_ms,
            pip_mirror=pip_mirror,
            mem_pool_mb=mem_pool_mb, import_cache_tree=import_cache_tree,
            sandbox_config=sandbox_config, docker_runtime=docker_runtime,
            limits=limits, features=features, trace=trace, storage=storage)

        self.ol = executable_path or self._find_executable()

        # Schema
        self.schema = schema or "http"
        self.hostname = hostname or "localhost"

        # Worker running info
        self._pid = None

    def _find_executable(self):
        """Try `which ol`. If fails, prompt the user to enter the path of open lambda."""
        if subprocess.call(["which", "ol"]) == 0:
            return "ol"
        logger.error("Please specify the path to openlambda executable (usually `ol`) in $PATH.")
        raise Exception("Please specify the path to openlambda executable (usually `ol`) in $PATH.")

    @property
    def base_url(self):
        """Construct the base url for various calls.
        Default: http://localhost:5000/
        """
        if self.port:
            base = f'{self.schema}://{self.hostname}:{str(self.port)}/'
        else:
            base = f'{self.schema}://{self.hostname}/'
        return base

    @property
    def port(self):
        return self.worker_port

    def _join(self, cmd):
        return ' '.join(cmd)

    # OpenLambda Commands
    @property
    def pid(self):
        pid_path = os.path.join(self.worker_dir, 'worker', 'worker.pid')
        if not os.path.exists(pid_path):
            if self._pid:
                logger.error(f'Expect pid={self._pid}, but worker.pid not found. '
                             f'Worker could be forcefully killed.')
                raise Exception(f'Expect pid={self._pid}, but worker.pid not found.')
            return None
        with open(pid_path, 'r') as f:
            pid = int(f.read())
            return pid

    # =====================================================================
    #  OpenLambda interface
    #     Use these method to statelessly communicate with openlambda.
    # =====================================================================

    def new(self):
        if os.path.exists(self.worker_dir):
            logger.warning(f'New: {self.worker_dir} exist.')
            return self

        cmd = [self.ol, 'new', f'--path={self.worker_dir}']
        logger.debug(f"Execute: {self._join(cmd)}")
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        proc.wait()

        if proc.returncode == 0:
            return self

        logger.error(f'{cmd} failed.')
        logger.error(stdout)
        raise Exception("ol new failed")

    def worker(self):
        """By default the worker is detached."""
        pid = self.pid
        if pid:
            return pid

        cmd = [self.ol, 'worker', '-d', f'--path={self.worker_dir}']
        logger.debug(f"Execute: {self._join(cmd)}")
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        proc.wait()
        if not stderr:
            return self.pid

        logger.error(f'Error while starting worker with cmd {self._join(cmd)}')
        logger.error(stderr)
        logger.debug(f'Full log:')
        logger.debug(stdout)
        logger.debug(stderr)
        raise Exception("Encounter error while starting worker.")

    def status(self):
        """Check the status of the worker."""
        url = os.path.join(self.base_url, self.STATUS)
        logger.info(f'Send status: curl -X POST {url}')
        res = requests.post(url)
        if res.status_code != 200:
            logger.warning(f'Worker does not exist at the end point.')
            return Status.STOP
        msg = res.text.strip()
        logger.info(f'Recv status: {msg}')
        # res.reason == 'OK'
        return Status.RUNNING

    def debug(self):
        """Send a debug message to the port"""
        url = os.path.join(self.base_url, self.DEBUG)
        logger.info(f'Send debug: curl -X POST {url}')
        res = requests.post(url)
        if res.status_code != 200:
            logger.warning(f'Worker does not exist at the end point.')
            return None
        msg = res.text.strip()
        logger.info(f'Recv debug: {msg}')
        return msg

    def run(self, endpoint: str, data=None):
        """
        Run a lambda experiment.
        Preferably we shall use a GO script to handle the multiprocessing overhead.
        """
        url = os.path.join(self.base_url, self.RUN, endpoint)
        logger.info(f'Run: curl -X POST {url}')
        res = requests.post(url, data=data)
        msg = res.text.strip()
        if res.status_code == 200:
            logger.info(f'Recv run/{endpoint}: {msg}')
            return msg
        if res.status_code == 500:
            logger.error(f'{endpoint} does not exist: {msg}')
            raise Exception(f"{endpoint} does not exist")
        logger.error(f'run/{endpoint} error with code {res.status_code}: {msg}')
        raise Exception(f"{endpoint} error with code {res.status_code}")

    # TODO: Batch run commands for benchmark. Use go script to do this
    def run_batch(self, ):
        """Launch a go script (bench.go) and batch run the experiments."""
        pass

    def kill(self):  # async=False, retry=100, sleep_time=1):
        """Send kill message to the currently running worker."""
        if not self.pid:
            logger.info("No worker.pid exist. Worker should be killed.")
            return Status.STOP

        status = self.status()
        if status == Status.STOP:
            logger.info("Worker already killed")
            return Status.STOP

        if status == Status.KILLING:
            # TODO: Add the killing information in open lambda interface
            logger.info("Worker still killing")
            return Status.KILLING

        cmd = [self.ol, 'kill', f'--path={self.worker_dir}']
        logger.debug(f"Execute: {self._join(cmd)}")
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        logger.debug(stdout)
        logger.debug(stderr)
        if stderr:
            raise Exception('Kill not cleaned')
        return Status.STOP

        # if async:
        #     logger.info('Kill async. Killing status can be further checked using `kill` or `status`.')
        #     return Status.KILLING
        #
        # i = 0
        # while i < retry:
        #     if proc.poll() is not None:
        #         break
        #     logger.info(f'Retry kill status: {}')
        #     sleep(sleep_time)
        #     i += 1
        # stdout, stderr = proc.communicate()

    def cleanup(self, path: str = None):
        """If kill is not successful, we need to manually clean up the skills.
        1. Use `lsof` to check the ol executables.
        2. Check
        """
        pass

    # =====================================================================
    #  Worker interface
    #     Use these method to create, destroy and run worker.
    # =====================================================================
    def mount(self):
        pass

    def umount(self):
        pass

    def remount(self):
        pass

    def register(self):
        """Register a script into worker registry"""
        pass

    def register_batch(self, workload: Workload):
        pass

    def run_workload(self, workload: Workload):
        pass

    def create(self):
        pass

    def start(self):
        pass

    def stop(self):
        pass

    def destroy(self):
        pass
