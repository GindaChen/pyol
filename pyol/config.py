import os
from os.path import abspath, join


class Options():
    """Provide options to a variable."""
    pass


class BaseConfig:
    """Config that can load/dump a dict."""

    def __str__(self):
        return str(self.to_dict())

    __repr__ = __str__

    def to_dict(self):
        result = {}
        for k, v in self.__dict__.items():
            if isinstance(v, BaseConfig):
                result[k] = v.to_dict()
            else:
                result[k] = v
        return result

    def from_dict(self, item):
        for k, v in item.items():
            if isinstance(self.__dict__[k], BaseConfig):
                self.__dict__[k].from_dict(v)
            else:
                self.__dict__[k] = v
        return self


class SandBox(Options):
    lambda_ = "lambda"
    sock = "sock"


Path_t = str


class Limits(BaseConfig):
    def __init__(self, procs=None, mem_mb=None, swappiness=None, installer_mem_mb=None):
        self.procs = procs or 10
        self.mem_mb = mem_mb or 50
        self.swappiness = swappiness or 0
        self.installer_mem_mb = installer_mem_mb or 500


class Features(BaseConfig):
    def __init__(self, reuse_cgroups=None, import_cache=None, downsize_paused_mem=None):
        self.reuse_cgroups = reuse_cgroups or False
        self.import_cache = import_cache or True
        self.downsize_paused_mem = downsize_paused_mem or True


class Trace(BaseConfig):
    def __init__(self, cgroups=None, memory=None, evictor=None, package=None):
        self.cgroups = cgroups or False
        self.memory = memory or False
        self.evictor = evictor or False
        self.package = package or False


class Storage(BaseConfig):
    def __init__(self, root=None, scratch=None, code=None):
        self.root = root or "private"
        self.scratch = scratch or ""
        self.code = code or ""


class ServerMode(Options):
    sock = "sock"
    lambda_ = "lambda"


class _Config(BaseConfig):
    def __init__(self, worker_dir: Path_t = None, worker_port: str = None, sandbox: str = None,
                 server_mode: str = None,
                 registry_cache_ms: int = None,
                 pip_mirror: str = None, mem_pool_mb: int = None, import_cache_tree: str = None,
                 sandbox_config: dict = None, docker_runtime: str = None,
                 limits: Limits = None, features: Features = None, trace: Trace = None, storage: Storage = None):
        """The config json data. The fields defined here will be directly written into a json file."""

        self.worker_dir: Path_t = worker_dir or abspath(os.getcwd())
        self.registry: Path_t = abspath(join(self.worker_dir, "registry"))
        self.Pkgs_dir: Path_t = abspath(join(self.worker_dir, "lambda/packages"))
        self.SOCK_base_path: Path_t = abspath(join(self.worker_dir, "lambda"))

        self.worker_port = worker_port or "5000"
        self.sandbox = sandbox or SandBox.lambda_
        self.server_mode = server_mode or ServerMode.sock

        self.registry_cache_ms = registry_cache_ms or 5000

        self.pip_mirror = pip_mirror or ""
        self.mem_pool_mb: int = mem_pool_mb or 2048
        self.import_cache_tree: str = import_cache_tree or ""

        self.sandbox_config: dict = sandbox_config or {}
        self.docker_runtime: str = docker_runtime or ""

        self.limits = limits or Limits()
        self.features = features or Features()
        self.trace = trace or Trace()
        self.storage = storage or Storage()


_config_data = _Config().__dict__


class Config(_Config):
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
        """Initialize the configs to run a worker."""

        super(Config, self).__init__(
            worker_dir=worker_dir, worker_port=worker_port,
            sandbox=sandbox, server_mode=server_mode,
            registry_cache_ms=registry_cache_ms,
            pip_mirror=pip_mirror,
            mem_pool_mb=mem_pool_mb, import_cache_tree=import_cache_tree,
            sandbox_config=sandbox_config, docker_runtime=docker_runtime,
            limits=limits, features=features, trace=trace, storage=storage)

        self.use_tmpfs = True


    def to_dict(self):
        disallow_keys = set(self.__dict__.keys()) - set(_config_data.keys())
        return {
            k: v for k, v in super(Config, self).to_dict()
            if k not in disallow_keys
        }
