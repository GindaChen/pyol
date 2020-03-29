import heapq
import json
import math
from collections import namedtuple

import numpy as np

# weights: vector of direct weight of each package; None means all equal
# prereq_first: pandas should be descendent of numpy (not vice versa)
# entropy_penalty: multiple weights by 1+penalty*entropy
# dist_weights: True means spread weight of pkg over all its prereqs
SplitOpts = namedtuple("SplitOpts", ["weights", "prereq_first", "entropy_penalty", "dist_weights"])


class Tree:
    # calls: matrix PxC (C is num of calls, P is num of pkgs)
    # deps: matrix PxP (a 1 in a cell means the col pkg depends on the row pkg)
    # weights: P-length vector of pkg weights
    def __init__(self, calls, deps, split_opts):
        if not split_opts.weights is None:
            self.weights = split_opts.weights
        else:
            self.weights = np.ones(len(list(deps)))

        P = len(self.weights)
        assert calls.shape[1] == P
        assert deps.shape[0] == P
        assert deps.shape[1] == P

        # distribute cost of a node over that node and all its prereqs
        if split_opts.dist_weights:
            self.weights = np.dot(deps / deps.sum(axis=0), self.weights)

        # column names, each of which is a package
        self.pkg_names = list(calls)
        self.deps = deps
        self.split_opts = split_opts

        # priority queue of best splits (we keep popping from the front to grow the tree)
        self.split_queue = []
        self.split_generation = 0

        # do last, because the Node deponds on other attrs of Tree
        self.root = Node(self, None, set(), calls.values)
        self.root.recursively_update_costs()
        self.root.enqueue_best_split()

    def calls_cost(self, calls):
        if calls.shape[0] == 0:
            return 0
        col_sums = calls.sum(axis=0)
        p = np.clip(col_sums / calls.shape[0], 1e-15, 1 - 1e-15)

        weights = self.weights
        if self.split_opts.entropy_penalty > 0:
            entropy = -(p * np.log2(p) + (1 - p) * np.log2(1 - p))
            weights = weights * (1 + self.split_opts.entropy_penalty * entropy)

        return np.dot(col_sums, weights)

    # perform the best n splits
    def do_splits(self, n):
        for i in range(n):
            if self.root.rcost == 0:
                break
            _, _, s = heapq.heappop(self.split_queue)
            before = s.node.rcost
            s.node.split(s)
            after = s.node.rcost
            assert math.isclose(s.benefit, before - after)

        return self

    def save(self, path):
        with open(path, "w") as f:
            f.write(self.root.json())


class Node:
    # packages:
    def __init__(self, tree, parent, packages, calls, depth=0):
        self.tree = tree
        self.parent = parent
        self.packages = packages
        self.calls = calls
        self.depth = depth

        if parent == None:
            self.name = "ROOT"
            self.remaining = ...
            assert (len(packages) == 0)
            self.remaining = tree.deps.values.sum(axis=0)
        else:
            self.name = "|".join(tree.pkg_names[p] for p in packages)
            self.remaining = np.array(self.parent.remaining)
            for p in packages:
                self.remaining -= tree.deps.iloc[p].values

        self.children = []
        self.cost = None
        self.rcost = None  # recursive (include cost of descendents)
        self.split_generation = self.tree.split_generation
        self.tree.split_generation += 1

    # update costs, from leaf to root
    def recursively_update_costs(self):
        self.cost = self.tree.calls_cost(self.calls)
        self.rcost = self.cost + sum(child.rcost for child in self.children)
        if self.parent:
            self.parent.recursively_update_costs()

    # this should be called:
    # 1. when a Node is first created
    # 2. when a Node gets a new child due to a split
    def enqueue_best_split(self):
        if self.cost < 0.01:
            return  # no point in splitting further

        best_split = None
        for pkg, rem in enumerate(self.remaining):
            # rem is the number of things we need to import to get
            # pkgi.  If rem=0, it means we've already imported the pkg.
            # if rem>1, it means we need to import some prereqs first.
            if rem < 1:
                continue
            if rem > 1 and self.tree.split_opts.prereq_first:
                continue

            split = Split(self, pkg)
            if best_split == None or split.benefit > best_split.benefit:
                best_split = split

        assert (best_split.benefit >= 0)

        # element 0: for actually scoring, element 1: to break ties, element 2: actual split
        tup = (-best_split.benefit, id(best_split), best_split)
        heapq.heappush(self.tree.split_queue, tup)

    def split(self, split):
        self.calls = split.parent_calls

        childPackages = {split.col}
        child = Node(self.tree, self, childPackages,
                     split.child_calls, self.depth + 1)
        self.children.append(child)
        child.recursively_update_costs()
        child.enqueue_best_split()
        self.enqueue_best_split()

    def print(self, *args, **kwargs):
        print("- " * self.depth, end="")
        print(*args, **kwargs)

    def dump(self):
        self.print("%s [%.1f total cost]" % (self.name, self.rcost))
        self.print("indirect pkgs: %s" % ",".join(self.packagesIndirect))
        self.print("%d calls with sub cost %.1f" % (len(self.calls), self.cost))
        self.print(len(self.children), "children with sub cost %.1f" % (self.rcost - self.cost))
        print()
        for c in self.children:
            c.dump()

    def to_dict(self):
        return {"packages": [self.tree.pkg_names[p] for p in self.packages],
                "children": [node.to_dict() for node in self.children],
                "split_generation": self.split_generation}

    def json(self, path=None):
        if path == None:
            return json.dumps(self.to_dict(), indent=2)
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)


class Split:
    def __init__(self, node, col):
        self.node = node
        self.col = col
        self.parent_calls = node.calls[node.calls[:, col] == 0]
        self.child_calls = node.calls[node.calls[:, col] == 1]
        self.child_calls[:, col] = 0

        self.parent_cost = self.node.tree.calls_cost(self.parent_calls)
        self.child_cost = self.node.tree.calls_cost(self.child_calls)
        # benefit is reduction in tree cost
        self.benefit = node.cost - (self.parent_cost + self.child_cost)

    def __str__(self):
        return "[on col %d of node %s to save %.1f]" % (self.col, self.node.name, self.benefit)
