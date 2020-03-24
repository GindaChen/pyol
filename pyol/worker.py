import os
from os.path import abspath

from pyol.config import Config, Path_t, Limits, Features, Trace, Storage


class Worker(Config):

    def __init__(self,
                 # Config native parameter
                 use_tmpfs=None,
                 # _Config parameters
                 worker_dir: Path_t = None, worker_port: str = None, sandbox: str = None,
                 server_mode: str = None,
                 registry_cache_ms: int = None,
                 pip_mirror: str = None, mem_pool_mb: int = None, import_cache_tree: str = None,
                 sandbox_config: dict = None, docker_runtime: str = None,
                 limits: Limits = None, features: Features = None, trace: Trace = None, storage: Storage = None):
        """The work directory will be a separated entity that eventually injected into config."""

        self.worker_dir = abspath(worker_dir) or abspath(os.getcwd())

        super(Worker, self).__init__(
            use_tmpfs=use_tmpfs,
            worker_dir=worker_dir, worker_port=worker_port,
            sandbox=sandbox, server_mode=server_mode,
            registry_cache_ms=registry_cache_ms,
            pip_mirror=pip_mirror,
            mem_pool_mb=mem_pool_mb, import_cache_tree=import_cache_tree,
            sandbox_config=sandbox_config, docker_runtime=docker_runtime,
            limits=limits, features=features, trace=trace, storage=storage)

