import os
from os.path import abspath, join


class OLError(BaseException):
    pass


class TypeError(OLError):
    def __init__(self, varname, expect, got):
        msg = "%s type error: Expect %s, got %s" % (varname, expect, got)
        super(TypeError, self).__init__(msg)


class ValueError(OLError):
    def __init__(self, varname, expect, got):
        expect = ', '.join(expect)
        msg = "%s type error: Expect {%s}, got %s" % (varname, expect, got)
        super(ValueError, self).__init__(msg)


class DependError(OLError):
    def __init__(self, varname, expect, got):
        expect = ', '.join(expect)
        got = ', '.join(got)
        msg = "%s dependency error: Expect [%s], got [%s]" % (varname, expect, got)
        super(DependError, self).__init__(msg)


class Path():
    expect_type = str

    def __call__(self, varname, var):
        if not isinstance(var, self.expect_type):
            raise TypeError(varname, self.expect_type, type(var))
        return var


class Reqd():
    def __init__(self, expect_type):
        self.expect_type = expect_type

    def __call__(self, varname, var):
        if callable(self.expect_type):
            return self.expect_type(varname, var)
        if isinstance(var, self.expect_type):
            return var
        raise TypeError(varname, self.expect_type, type(var))


class Optional():
    def __init__(self, default_value):
        self.default_value = default_value
        self.expect_type = type(default_value)

    def __call__(self, varname, var=None):
        if not var:
            if callable(self.default_value):
                return self.default_value()
            return self.default_value

        # var is specified
        if callable(self.expect_type):
            return self.expect_type(varname, var)
        if isinstance(var, self.expect_type):
            return var
        raise TypeError(varname, self.expect_type, type(var))


class Depend():
    """Variable is optionally defined. Default value depend on other variable."""

    # TODO: Can use insepct to refactor it

    def __init__(self, depends, expect_type, constructor: callable):
        self.depends = set(depends)
        assert all(isinstance(t, str) for t in self.depends)
        self.expect_type = expect_type
        self.constructor = constructor
        assert callable(constructor)

    def __call__(self, varname, var=None, **kwargs):
        if not var:
            dwargs = {d: v for d, v in kwargs.items() if d in self.depends}
            if set(dwargs.keys()) != self.depends:
                raise DependError(varname, expect=self.depends, got=kwargs.keys())
            var = self.constructor(**kwargs)

        if callable(self.expect_type):
            return self.expect_type(varname, var)
        if isinstance(var, self.expect_type):
            return var
        raise TypeError(varname, self.expect_type, type(var))


class OneOf():
    def __init__(self, *args):
        self.options = args

    def __call__(self, varname, var=None):
        if not var:
            return self.options[0]

        if var not in self.options:
            raise ValueError(varname, self.options, var)
        return var


metainfo = {
    "worker_dir": Reqd(str),
    "worker_port": "5000",
    "sandbox": "sock",
    "server_mode": OneOf("lambda", "sock"),
    "registry": Depend(
        "worker_dir", str,
        lambda worker_dir="": abspath(join(worker_dir, "registry"))
    ),
    "registry_cache_ms": 5000,
    "Pkgs_dir": Depend(
        "worker_dir", str,
        lambda worker_dir="": abspath(join(worker_dir, "lambda/packages"))
    ),
    "pip_mirror": "",
    "mem_pool_mb": Optional(2048),  # TODO: Add
    "import_cache_tree": "",
    "SOCK_base_path": Depend(
        "worker_dir", str,
        lambda worker_dir="": abspath(join(worker_dir, "lambda"))
    ),
    "sandbox_config": {},
    "docker_runtime": "",
    "limits": {
        "procs": 10,
        "mem_mb": 50,
        "swappiness": 0,
        "installer_mem_mb": 500
    },
    "features": {
        "reuse_cgroups": False,
        "import_cache": True,
        "downsize_paused_mem": True
    },
    "trace": {
        "cgroups": False,
        "memory": False,
        "evictor": False,
        "package": False
    },
    "storage": {
        "root": "private",
        "scratch": "",
        "code": ""
    }
}


##-----------------------
## config.py Generator
##-----------------------

class generate():


    pass