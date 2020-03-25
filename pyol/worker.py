import os
from os.path import abspath

from pyol.config import Config, Path_t, Limits, Features, Trace, Storage
from pyol import ol, setup_ol, OL, default_ol

ol = default_ol


class Worker(Config):

    def __init__(
            self,
            # Config native parameter
            use_tmpfs: bool = None,
            # _Config parameters
            worker_dir: Path_t = None,
            worker_port: str = None, sandbox: str = None,
            server_mode: str = None,
            registry_cache_ms: int = None,
            pip_mirror: str = None, mem_pool_mb: int = None, import_cache_tree: str = None,
            sandbox_config: dict = None, docker_runtime: str = None,
            limits: Limits = None, features: Features = None, trace: Trace = None, storage: Storage = None):
        """The work directory will be a separated entity that eventually injected into config."""

        # TODO: Inject the worker_dir to the setting of config.
        worker_dir = abspath(worker_dir) or abspath(join(os.getcwd(), "default-ol"))
        self.worker_dir = worker_dir

        super(Worker, self).__init__(
            use_tmpfs=use_tmpfs,
            worker_dir=worker_dir, worker_port=worker_port,
            sandbox=sandbox, server_mode=server_mode,
            registry_cache_ms=registry_cache_ms,
            pip_mirror=pip_mirror,
            mem_pool_mb=mem_pool_mb, import_cache_tree=import_cache_tree,
            sandbox_config=sandbox_config, docker_runtime=docker_runtime,
            limits=limits, features=features, trace=trace, storage=storage)


    def mount(self):
        pass

    def umount(self):
        pass

    def remount(self):
        pass

    def create():
        pass

    def destroy():
        pass

    def start():
        pass

    def stop():
        pass
    