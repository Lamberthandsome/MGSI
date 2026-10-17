"""Deterministic centroid KD-tree with bounds covering complete chunks.

Range queries walk the tree with box-overlap tests against the stored
bounds. This module needs only the Python standard library.
Coordinates must describe finite, non-wrapping closed boxes in one encoding.
"""
from collections import namedtuple
import math


class Chunk(namedtuple("ChunkFields", "identifier low high representative")):
    __slots__ = ()

    def __new__(cls, identifier, low, high, representative):
        return super(Chunk, cls).__new__(
            cls, str(identifier), tuple(float(x) for x in low),
            tuple(float(x) for x in high), tuple(float(x) for x in representative))


class Node:
    __slots__ = ("chunk", "low", "high", "depth", "left", "right")

    def __init__(self, chunk, low, high, depth):
        self.chunk = chunk
        self.low = low
        self.high = high
        self.depth = depth
        self.left = None
        self.right = None


def _validate_box(low, high, dimensions):
    if len(low) != dimensions or len(high) != dimensions or dimensions < 1:
        raise ValueError("Box dimensions must agree and be positive")
    if not all(math.isfinite(x) for x in low + high):
        raise ValueError("Coordinates must be finite")
    if not all(a <= b for a, b in zip(low, high)):
        raise ValueError("Lower bounds must not exceed upper bounds")


def intersects(low, high, query_low, query_high):
    """Inclusive box intersection; boundary contact counts as intersection."""
    return not any(b < c or a > d for a, b, c, d in
                   zip(low, high, query_low, query_high))


class ChunkTree:
    """No balance guarantee; construction and traversal use explicit stacks."""

    def __init__(self, chunks, dimensions=None):
        chunks = list(chunks)
        self.dimensions = (len(chunks[0].low) if chunks else 0) if dimensions is None else dimensions
        self.root = None
        self.node_count = 0
        self.maximum_depth = -1
        if len({c.identifier for c in chunks}) != len(chunks):
            raise ValueError("Chunk identifiers must be unique")
        k = self.dimensions
        for chunk in chunks:
            _validate_box(chunk.low, chunk.high, k)
            if len(chunk.representative) != k or not all(math.isfinite(x) for x in chunk.representative):
                raise ValueError("Representative dimensions must agree and be finite")
            if not all(a <= p <= b for a, b, p in zip(chunk.low, chunk.high, chunk.representative)):
                raise ValueError("Representative must lie within its complete chunk bounds")

        # One initial ID sort; stable partitions retain this order at all depths.
        chunks.sort(key=lambda c: c.identifier)
        stack = [(chunks, 0, None, None)] if chunks else []
        while stack:
            group, depth, parent, side = stack.pop()
            centroid = tuple(math.fsum(c.representative[d] / len(group) for c in group)
                             for d in range(k))
            pivot = min(group, key=lambda c: (
                math.fsum((c.representative[d] - centroid[d]) ** 2 for d in range(k)),
                c.identifier))
            low = tuple(min(c.low[d] for c in group) for d in range(k))
            high = tuple(max(c.high[d] for c in group) for d in range(k))
            node = Node(pivot, low, high, depth)
            self.node_count += 1
            self.maximum_depth = max(self.maximum_depth, depth)
            if parent is None:
                self.root = node
            else:
                setattr(parent, side, node)
            dimension = depth % k
            left, right = [], []
            for chunk in group:
                if chunk.identifier == pivot.identifier:
                    continue
                (left if chunk.representative[dimension] < pivot.representative[dimension]
                 else right).append(chunk)
            # Removing the pivot ensures progress even for identical coordinates.
            if right:
                stack.append((right, depth + 1, node, "right"))
            if left:
                stack.append((left, depth + 1, node, "left"))

    def query(self, low, high, stats=None):
        low, high = tuple(float(x) for x in low), tuple(float(x) for x in high)
        _validate_box(low, high, self.dimensions or len(low))
        visited, pruned = 0, 0
        matches = []
        stack = [self.root] if self.root is not None else []
        while stack:
            node = stack.pop()
            visited += 1
            if not intersects(node.low, node.high, low, high):
                pruned += 1
                continue
            chunk = node.chunk
            if intersects(chunk.low, chunk.high, low, high):
                matches.append(chunk.identifier)
            if node.right is not None:
                stack.append(node.right)
            if node.left is not None:
                stack.append(node.left)
        if stats is not None:
            stats.update(visited_nodes=visited, pruned_subtrees=pruned, reported_chunks=len(matches))
        # Deterministic traversal avoids sorting results; query cost is O(kN + m).
        return matches
