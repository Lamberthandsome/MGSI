"""Local correctness checks; no raster reads, network, or performance timings."""
from pathlib import Path
import ast
import hashlib
import json
import math
import random
import sys
import unittest

sys.dont_write_bytecode = True
CODE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CODE))
from mgsi_common.chunk_tree import Chunk, ChunkTree
from mgsi_common.index import GlobalRangeIndex, MGSIIndex

COUNTS = {"query_comparisons": 0, "subtree_checks": 0, "entrypoints_checked": []}


def reference(chunks, low, high):
    # Independent full scan, without using the implementation's intersection helper.
    result = []
    for chunk in chunks:
        overlap = True
        for d in range(len(low)):
            if max(chunk.low[d], low[d]) > min(chunk.high[d], high[d]):
                overlap = False
                break
        if overlap:
            result.append(chunk.identifier)
    return sorted(result)


def shape(tree):
    result, stack = [], [tree.root]
    while stack:
        node = stack.pop()
        if node is None:
            result.append(None)
        else:
            result.append((node.chunk.identifier, node.depth, node.low, node.high))
            stack.extend([node.right, node.left])
    return result


class GlobalIndexTests(unittest.TestCase):
    def compare(self, tree, chunks, low, high):
        found = tree.query(low, high)
        self.assertEqual(sorted(found), reference(chunks, low, high))
        self.assertEqual(len(found), len(set(found)))
        COUNTS["query_comparisons"] += 1

    def check_subtrees(self, tree):
        if tree.root is None:
            return
        stack, descendants = [(tree.root, False)], {}
        while stack:
            node, expanded = stack.pop()
            if not expanded:
                stack.append((node, True))
                stack.extend((child, False) for child in [node.left, node.right] if child is not None)
                continue
            group = [node.chunk]
            for child in [node.left, node.right]:
                if child is not None:
                    group.extend(descendants[child])
            descendants[node] = group
            self.assertEqual(node.low, tuple(min(c.low[d] for c in group) for d in range(tree.dimensions)))
            self.assertEqual(node.high, tuple(max(c.high[d] for c in group) for d in range(tree.dimensions)))
            centroid = [sum(c.representative[d] for c in group) / len(group)
                        for d in range(tree.dimensions)]
            expected = min(group, key=lambda c: (
                sum((c.representative[d] - centroid[d]) ** 2 for d in range(tree.dimensions)),
                c.identifier))
            self.assertEqual(node.chunk.identifier, expected.identifier)
            d = node.depth % tree.dimensions
            for child, left in [(node.left, True), (node.right, False)]:
                if child is not None:
                    self.assertEqual(child.depth, node.depth + 1)
                    for c in descendants[child]:
                        self.assertEqual(c.representative[d] < node.chunk.representative[d], left)
            COUNTS["subtree_checks"] += 1
        self.assertEqual(len(descendants[tree.root]), tree.node_count)

    def test_random_2d_3d_against_full_scan(self):
        rng = random.Random(1718)
        for dimensions in [2, 3]:
            for count in [1, 2, 17, 80, 200]:
                chunks = []
                for i in range(count):
                    low = tuple(rng.randint(-100, 100) for _ in range(dimensions))
                    high = tuple(a + rng.randint(0, 35) for a in low)
                    # Include lower-corner, centre, and upper-corner representatives.
                    point = [low, tuple((a + b) / 2 for a, b in zip(low, high)), high][i % 3]
                    chunks.append(Chunk(str(i), low, high, point))
                tree = ChunkTree(chunks)
                self.check_subtrees(tree)
                for _ in range(160):
                    low = tuple(rng.randint(-130, 130) for _ in range(dimensions))
                    high = tuple(a + rng.randint(0, 70) for a in low)
                    self.compare(tree, chunks, low, high)

    def test_extents_outside_representative_and_touching_edges(self):
        chunks = [Chunk("long", (-100, -1), (100, 1), (-100, -1)),
                  Chunk("edge", (100, 1), (102, 3), (100, 1)),
                  Chunk("point", (0, 0), (0, 0), (0, 0))]
        tree = ChunkTree(chunks)
        for low, high in [((0, 0), (0, 0)), ((100, 1), (100, 1)),
                          ((101, 2), (101, 2)), ((103, 3), (104, 4))]:
            self.compare(tree, chunks, low, high)
        self.assertIn("long", tree.query((0, 0), (0, 0)))

    def test_stable_ties_and_recursive_pivots(self):
        chunks = [Chunk(name, (x, 0), (x, 0), (x, 0))
                  for name, x in [("z", -3), ("b", -1), ("a", 1), ("y", 3)]]
        baseline = shape(ChunkTree(chunks))
        self.assertEqual(ChunkTree(chunks).root.chunk.identifier, "a")
        rng = random.Random(18)
        for _ in range(30):
            rng.shuffle(chunks)
            tree = ChunkTree(chunks)
            self.assertEqual(shape(tree), baseline)
            self.check_subtrees(tree)

    def test_identical_points_beyond_recursion_limit(self):
        chunks = [Chunk(f"{i:04d}", (0, 0), (0, 0), (0, 0)) for i in range(1200)]
        tree = ChunkTree(chunks)
        self.assertEqual(tree.node_count, 1200)
        self.assertEqual(tree.maximum_depth, 1199)
        self.compare(tree, chunks, (0, 0), (0, 0))
        self.compare(tree, chunks, (1, 1), (2, 2))

    def test_actual_subtree_pruning(self):
        chunks = [Chunk(str(i), (i, i), (i, i), (i, i)) for i in range(100)]
        tree = ChunkTree(chunks)
        stats = {}
        self.assertEqual(tree.query((200, 200), (201, 201), stats), [])
        self.assertEqual(stats, {"visited_nodes": 1, "pruned_subtrees": 1, "reported_chunks": 0})
        self.compare(tree, chunks, (0, 0), (0, 0))
        tree.query((0, 0), (0, 0), stats)
        self.assertLess(stats["visited_nodes"], tree.node_count)
        self.assertGreater(stats["pruned_subtrees"], 0)

    def test_empty_singleton_and_rebuild(self):
        self.assertEqual(ChunkTree([]).query((0, 0), (1, 1)), [])
        index = MGSIIndex()
        meta = {"chunk_id": "single", "lon_min": -5, "lon_max": 5, "lat_min": -5, "lat_max": 5}
        query = {"lon_min": 0, "lon_max": 0, "lat_min": 0, "lat_max": 0}
        index.build_global_index([meta])
        self.assertIs(index.query_global_index(query)[0], meta)
        index.build_global_index([])
        self.assertEqual(index.query_global_index(query), [])

    def test_invalid_metadata_and_query_are_rejected(self):
        good = Chunk("same", (0, 0), (1, 1), (0, 0))
        bad = [Chunk("bad", (1, 0), (0, 1), (0, 0)),
               Chunk("bad", (0,), (1,), (0, 0)),
               Chunk("bad", (0, 0), (1, 1), (2, 0)),
               Chunk("bad", (0, 0), (math.inf, 1), (0, 0)),
               Chunk("bad", (0, 0), (1, 1), (math.nan, 0))]
        for chunk in bad:
            with self.assertRaises(ValueError):
                ChunkTree([chunk])
        with self.assertRaises(ValueError):
            ChunkTree([good, good])
        for tree in [ChunkTree([good]), ChunkTree([], dimensions=2)]:
            for low, high in [((0,), (1,)), ((1, 0), (0, 1)), ((0, 0), (math.nan, 1))]:
                with self.assertRaises(ValueError):
                    tree.query(low, high)

    def test_metadata_adapter_3d_and_atomic_failure(self):
        index = MGSIIndex(axes=("lon", "lat", "time"))
        meta = {"chunk_id": "time-block", "lon_min": -10, "lon_max": 10,
                "lat_min": -10, "lat_max": 10, "time_min": 100, "time_max": 200}
        query = {"lon_min": 0, "lon_max": 0, "lat_min": 0, "lat_max": 0,
                 "time_min": 200, "time_max": 220}
        index.build_global_index([meta])
        self.assertEqual(index.query_global_index(query), [meta])
        with self.assertRaises(ValueError):
            index.build_global_index([meta, meta])
        self.assertEqual(index.query_global_index(query), [meta])
        query.update(time_min=201)
        self.assertEqual(index.query_global_index(query), [])

    def test_current_entrypoint_global_index_classes(self):
        for relative in ["time_range/local_mgsi.py", "tiff/local_mgsi.py",
                         "ablation/processor_complete.py", "ablation/mgsi1.py",
                         "ablation/mgsi3.py", "ablation/no_repart.py"]:
            source = (CODE / relative).read_text(encoding="utf-8")
            parsed = ast.parse(source)
            classes = [n for n in parsed.body if isinstance(n, ast.ClassDef) and n.name == "MGSIIndex"]
            if classes:
                namespace = {"GlobalRangeIndex": GlobalRangeIndex}
                # Execute the actual index class without importing experiment
                # dependencies or starting its top-level experiment code.
                exec(compile(ast.Module(body=classes, type_ignores=[]), relative, "exec"), namespace)
                index = namespace["MGSIIndex"]()
            else:
                self.assertTrue(any(isinstance(n, ast.ImportFrom) and n.module == "mgsi_common.index"
                                    and any(a.name == "MGSIIndex" for a in n.names) for n in parsed.body))
                index = MGSIIndex()
            identifier_field = "file_id" if relative.endswith("mgsi1.py") else "chunk_id"
            meta = {identifier_field: "wide", "lon_min": -100, "lon_max": 100,
                    "lat_min": -1, "lat_max": 1, "file_path": "not-opened.nc4"}
            index.build_global_index([meta])
            self.assertEqual(index.query_global_index({"lon_min": 0, "lon_max": 0,
                                                       "lat_min": 0, "lat_max": 0}), [meta])
            if relative.endswith("mgsi3.py"):
                self.assertFalse(hasattr(index, "local_hash"))
            COUNTS["entrypoints_checked"].append(relative)


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(GlobalIndexTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if "--report" in sys.argv:
        target = Path(sys.argv[sys.argv.index("--report") + 1])
        target.write_text(json.dumps({
            "status": "PASS" if result.wasSuccessful() else "FAIL",
            "tests_run": result.testsRun, "failures": len(result.failures), "errors": len(result.errors),
            **COUNTS, "maximum_degenerate_fixture": 1200,
            "network_access": False, "server_access": False, "raster_reads": False,
            "performance_benchmark": False, "implementation": str(CODE / "mgsi_common/chunk_tree.py"),
            "implementation_sha256": hashlib.sha256((CODE / "mgsi_common/chunk_tree.py").read_bytes()).hexdigest(),
        }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    raise SystemExit(0 if result.wasSuccessful() else 1)
