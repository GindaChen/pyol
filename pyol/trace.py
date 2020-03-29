import json
from typing import List, Dict, Set

import numpy as np
import pandas as pd


class Package():
    def __init__(self, name, top, deps, type="packaage"):
        # {
        #   "deps": ["pytz", "python-dateutil", "numpy"],
        #   "name": "pandas",
        #   "type": "package",
        #   "top": ["pandas"]
        # }

        self.name: str = name
        self.top: list = top
        self.deps: list = deps
        self.type: str = type


class Trace:
    def __init__(self):
        # each package object is a dict, something like this:
        self.packages: List[Package] = []
        self.packagesDict = {}

        self.pdeps: Dict[str, Set[str]] = {}
        self.fdeps: Dict[str, List[str]] = {}

    def load(self, path):
        with open(path, "r") as f:
            data = [json.loads(l) for l in f]

        # Transform each row in data to a Package object
        for row in data:
            if row["type"] != "package":
                continue
            deps, name, type, top = row['deps'], row['name'], row['type'], row['top']
            pkg = Package(name, top, deps, type)
            self.packages.append(pkg)
            self.packagesDict[name] = pkg

        pdeps = self.indirect_deps(self.packages)
        fdeps = self.function_deps(self.packages, pdeps)
        self.pdeps = pdeps
        self.fdeps = fdeps
        return self

    def save(self, path):
        with open(path, 'w') as f:
            for pkg in self.packages:
                f.write(json.dumps({
                    "name": pkg.name,
                    "deps": pkg.deps,
                    "top": pkg.top,
                    "type": pkg.type,
                }) + "\n")

    def indirect_deps(self, packages: List[Package]) -> Dict[str, Set[str]]:
        """Get indirect dependency relation. A simple union algorithm that
        discovers the acyclic graph from the dependency relation"""

        # Initialize pdeps to all direct dependencies of packages
        pdeps = {}
        for pkg in packages:
            pdeps[pkg.name] = set(pkg.deps)

        # Loop until no package's dep relation changes
        #   for each package k in pdeps
        #       discover the dependency changes
        changes = True
        while changes:
            changes = False
            for k in pdeps:
                k2_list = list(pdeps[k])
                before = len(pdeps[k])
                for k2 in k2_list:
                    pdeps[k] |= pdeps[k2]
                if len(pdeps[k]) != before:
                    changes = True
        return pdeps

    def function_deps(self, packages: List[Package], pdeps: Dict[str, Set[str]]) -> Dict[str, Set[str]]:
        """Get function dependencies."""
        fdeps = {}
        for pkg in packages:
            if pkg.type != 'function':
                continue
            deps = set(pkg.deps)
            for d in deps:
                deps |= pdeps[d]
            fdeps[pkg.name] = deps
        return fdeps

    def call_matrix(self):
        df_rows = []
        for pkg in self.packages:
            if pkg.type == "invocation":
                df_row = {k: 1 for k in self.fdeps[pkg.name]}
                df_rows.append(df_row)
        df = pd.DataFrame(df_rows).fillna(0).astype(int)
        return df[sorted(df.columns)]

    def dep_matrix(self):
        pnames = sorted(self.pdeps.keys())
        df = pd.DataFrame(index=pnames, columns=pnames).fillna(0)
        for pkg, prereqs in self.pdeps.items():
            df.loc[pkg, pkg] = 1
            for prereq in prereqs:
                df.loc[prereq, pkg] = 1
        return df

    def cost_vector(self, path, stat='ms'):
        pnames = sorted(self.pdeps.keys())
        with open(path) as f:
            costs = json.load(f)
        return np.array([costs[p][stat] for p in pnames])
