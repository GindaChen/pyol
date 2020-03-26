import json
from os.path import abspath
from random import random
from typing import List

from pyol.utils import logger

# The packages that found popular on Github
direct_packages = [
    'pandas', 'scipy', 'matplotlib', 'sqlalchemy',
    'django', 'flask', 'numpy', 'simplejson', 'protobuf', 'jinja2',
    'pip', 'setuptools', 'requests', 'mock', 'werkzeug', 'dnspython', 'six', 'PyQt5'
]


class Workload:
    """Define the json structure of a workflow:
    json_dct = {
        "funcs": [{"name": str, "code": List[str]}],
        "calls": [{"name":str, "data": OptionalDict}]
    }
    """

    def __init__(self):
        # Register functions into open lambda.
        self.funcs = []
        # Calls into open lambda
        self.calls = []

    def new_name(self):
        return f'fn{len(self.funcs)}'

    def addFunc(self, packages: List[str] = None, imports: List[str] = None):
        name = self.new_name()
        code = []
        if packages:
            packages = ', '.join(packages)
            code.append(f'# ol-install: {packages}\n')
        if imports:
            imports = ', '.join(imports)
            code.append(f'import {imports}\n')
        code.append(f'def f(event):\n')
        code.append(f'    return {name}')

        self.funcs.append({
            "name": name,
            "code": code
        })
        return self

    def addCall(self, name):
        self.calls.append({"name": name})
        # self.calls.append({"name": name, "data": {}})

    def load(self, path):
        with open(path) as f:
            dct = json.load(f)
            self.funcs = dct.get('funcs', [])
            self.calls = dct.get('calls', [])
        logger.info(f'Load workload from {abspath(path)}')

    def dump(self, path):
        with open(path, 'w+') as f:
            json.dump({
                'calls': self.calls,
                'funcs': self.funcs
            }, f, indent=2)
        logger.info(f'Dump workload to {abspath(path)}')


def gen_workload_each_once(packages_names=None):
    """Generate a function call for each package."""
    w = Workload()
    if not packages_names:
        return w
    for p in packages_names:
        name = w.addFunc([p], [])
        w.addCall(name)
    return w

# # TODO: Fix this
# def gen_workload_pairs(packages=None, calls=500):
#     """Randomly sample a pair of packages"""
#     w = Workload()
#     for i in range(calls):
#         pkgs = random.sample(packages, 2)
#         name = w.addFunc([pkgs[0]["name"], pkgs[1]["name"]], pkgs[0]["top"] + pkgs[1]["top"])
#         w.addCall(name)
#     return w
#
# # TODO: Fix this
# def gen_workload_pairs_skewed(packages=None, calls=500):
#     w = Workload()
#
#     # duplicate each package 1, 2, or 3 times (to simulate different popularity)
#     dups = []
#     for p in packages:
#         r = random.randint(1, 3)
#         for i in range(r):
#             dups.append(p)
#     packages = dups
#
#     for i in range(calls):
#         pkgs = []
#         while len(set(pkg["name"] for pkg in pkgs)) != 2:
#             pkgs = random.sample(packages, 2)
#         name = w.addFunc([
#             pkgs[0]["name"], pkgs[1]["name"]],
#             pkgs[0]["top"] + pkgs[1]["top"]
#         )
#         w.addCall(name)
#     return w
