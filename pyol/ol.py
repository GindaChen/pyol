import os
import subprocess
from enum import Enum

import requests

from pyol.utils import logger


class Status(Enum):
    OK = 0
    KILLING = 1
    STOP = 2


class OL():
    """Stateless interface that communicates to the go command line tool."""

    STATUS = 'status'
    DEBUG = 'debug'
    RUN = 'run'

    def __init__(self, worker_dir: str = None, executable_path: str = None,
                 schema="http", hostname="localhost", port="5000"):
        # Get the `ol` executable path
        self.ol = executable_path or self._find_executable()

        # Set the open lambda path
        self._worker_dir = worker_dir

        # Schema
        self.schema = schema
        self.hostname = hostname
        self.port = port

    def _join(self, cmd):
        return ' '.join(cmd)

    @property
    def worker_dir(self):
        if not self._worker_dir:
            logger.error('Please set worker_dir before using it!')
            raise Exception()
        return self._worker_dir

    @worker_dir.setter
    def worker_dir(self, item):
        self._worker_dir = item

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

    def _find_executable(self):
        """Try `which ol`. If fails, prompt the user to enter the path of open lambda."""
        if subprocess.call(["which", "ol"]) == 0:
            return "ol"
        logger.error("Please specify the path to openlambda executable (usually `ol`) in $PATH.")
        raise Exception("Please specify the path to openlambda executable (usually `ol`) in $PATH.")

    def new(self, remove_if_exist=False):
        if os.path.exists(self.worker_dir):
            pass

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

    @property
    def pid(self):
        pid_path = os.path.join(self.worker_dir, 'worker', 'worker.pid')
        if not os.path.exists(pid_path):
            return None
        with open(pid_path, 'r') as f:
            pid = int(f.read())
            return pid

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
        return Status.OK

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

    def batch_run(self, ):
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


default_ol = OL(worker_dir="./default-ol", executable_path="ol")


def setup_ol(worker_dir, ol_path):
    """Setup a new ol object, possibly pointing to a different executable."""
    ol = OL(worker_dir=worker_dir, executable_path=ol_path)
    return ol
