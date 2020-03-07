import os
import json


class BaseConfig():
    def __init__(self):
        # Freeze the type of the variable.
        self.__frozen__ = None
        self.__freeze__()

    def __freeze__(self):
        self.__frozen__ = {}
        for key, value in self.__dict__.items():
            if key.startswith("_"):
                continue
            # TODO: Complex type shall be careful
            self.__frozen__[key] = type(value)

    def __check_one__(self, value, type_):
        if type_ in [set]:
            return value in type_
        if isinstance(value, type_):
            return True
        return False

    def __check__(self):
        result = True
        for key, value in self.__dict__.items():
            if key.startswith("_"):
                continue
            expect = self.__frozen__[key]
            result = self.__check_one__(value, expect) and result
        return result

    def __call__(self, *args, **kwargs):
        self.__check__()
        result = {}
        for key, value in self.__dict__.items():
            if key.startswith("_"):
                continue
            if isinstance(value, BaseConfig):
                value = value(*args, **kwargs)
            result[key] = value
        return result

    def __repr__(self):
        return self()

    def tojson(self):
        config = self()
        return json.dumps(config, indent=2)


class LimitsConfig(BaseConfig):
    this_key = "limits"

    def __init__(self):
        # how many processes can be created within a Sandbox?
        self.procs = 10

        # how much memory can a regular lambda use?  The lambda can
        # always set a lower limit for itself.
        self.mem_mb = 50

        # how aggresively will the mem of the Sandbox be swapped?
        self.swappiness = 0

        # how much memory do we use for an admin lambda that is used
        # for pip installs?
        self.installer_mem_mb = 500
        super(LimitsConfig, self).__init__()


class FeaturesConfig(BaseConfig):
    this_key = "features"

    def __init__(self):
        self.reuse_cgroups = False
        self.import_cache = True
        self.downsize_paused_mem = True
        super(FeaturesConfig, self).__init__()


class TraceConfig(BaseConfig):
    this_key = "trace"

    def __init__(self):
        self.cgroups = False
        self.memory = False
        self.evictor = False
        self.package = False
        super(TraceConfig, self).__init__()

    def toggle_all(self):
        self.cgroups = not self.cgroups
        self.memory = not self.memory
        self.evictor = not self.evictor
        self.package = not self.package


class StorageConfig(BaseConfig):
    this_key = "storage"

    def __init__(self):
        self.root = "private"
        self.scratch = ""
        self.code = ""
        super(StorageConfig, self).__init__()

    def __setattr__(self, key, value):
        if key == "root":
            assert value in ["", "memory", "private"]
        self.__dict__[key] = value


class SandboxConfig(BaseConfig):
    this_key = "sandbox_config"

    def __init__(self):
        super(SandboxConfig, self).__init__()


class Config(BaseConfig):
    def __init__(self, worker_dir: str):
        assert isinstance(worker_dir, str)
        worker_dir = os.path.abspath(worker_dir)

        self.worker_dir = worker_dir
        self.registry = os.path.join(worker_dir, "registry")
        self.Pkgs_dir = os.path.join(worker_dir, "lambda/packages")
        self.SOCK_base_path = os.path.join(worker_dir, "lambda")

        self.worker_port = "5000"
        self.sandbox = "sock"  # TODO: Make it an option to select
        self.server_mode = "lambda"  # TODO: Make it an option to select
        self.registry_cache_ms = 5000
        self.pip_mirror = ""
        self.mem_pool_mb = 2048
        self.import_cache_tree = ""

        self.sandbox_config = SandboxConfig()
        self.docker_runtime = ""

        self.limits = LimitsConfig()
        self.features = FeaturesConfig()
        self.trace = TraceConfig()
        self.storage = StorageConfig()

        super(Config, self).__init__()

    def __setattr__(self, key, value):
        if key == "sandbox":
            assert value in ["sock", "docker"]
        elif key == "server_mode":
            assert value in ["lambda", "sock"]
        self.__dict__[key] = value

    def rebase_worker_dir(self, worker_dir):
        # Change all path-related variables
        self.worker_dir = worker_dir
        self.registry = os.path.join(worker_dir, "registry")
        self.Pkgs_dir = os.path.join(worker_dir, "lambda/packages")
        self.SOCK_base_path = os.path.join(worker_dir, "lambda")


class LambdaConfig(Config):
    def __init__(self, worker_dir: str):
        super(LambdaConfig, self).__init__(worker_dir)
        # self.sandbox = "sock"
        self.server_mode = "lambda"


class SOCKConfig(Config):
    def __init__(self, worker_dir: str):
        super(SOCKConfig, self).__init__(worker_dir)
        # self.sandbox = "sock"
        self.server_mode = "sock"
