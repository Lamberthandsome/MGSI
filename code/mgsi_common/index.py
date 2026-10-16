"""Global range index and its adapters for the MGSI query pipelines.

Global candidates use complete-chunk subtree bounds and deterministic centroid
pivots. Local hash tables and the separate in-memory point KD-trees are owned
by the callers. Optional time coordinates must use the same numeric encoding
in metadata and queries; the existing spatial adapter defaults to lon/lat.
"""
from mgsi_common.chunk_tree import Chunk, ChunkTree


class GlobalRangeIndex:
    def _init_global_index(self, axes=("lon", "lat"), representative="lower"):
        self.axes = tuple(axes)
        if not self.axes or len(set(self.axes)) != len(self.axes):
            raise ValueError("Indexed axes must be unique and nonempty")
        if representative not in ("lower", "centre"):
            raise ValueError("Unknown representative policy")
        self.representative_policy = representative
        self.global_kdtree = ChunkTree([], dimensions=len(self.axes))
        self.global_points = []
        self.global_metadata = []
        self._metadata_by_id = {}

    def build_global_index(self, chunks_metadata):
        metadata = list(chunks_metadata)
        chunks, mapping = [], {}
        for meta in metadata:
            identifier = meta.get("chunk_id", meta.get("file_id"))
            if identifier is None:
                raise ValueError("Each metadata entry requires a chunk_id or file_id")
            identifier = str(identifier)
            low = tuple(meta[axis + "_min"] for axis in self.axes)
            high = tuple(meta[axis + "_max"] for axis in self.axes)
            point = (low if self.representative_policy == "lower" else
                     tuple(float(a) / 2 + float(b) / 2 for a, b in zip(low, high)))
            chunks.append(Chunk(identifier, low, high, point))
            mapping[identifier] = meta
        tree = ChunkTree(chunks, dimensions=len(self.axes))
        # Publish together, so invalid metadata cannot leave a half-built index.
        self.global_kdtree = tree
        self.global_metadata = metadata
        self.global_points = [chunk.representative for chunk in chunks]
        self._metadata_by_id = mapping

    def query_global_index(self, bbox, stats=None):
        low = tuple(bbox[axis + "_min"] for axis in self.axes)
        high = tuple(bbox[axis + "_max"] for axis in self.axes)
        return [self._metadata_by_id[identifier] for identifier in
                self.global_kdtree.query(low, high, stats=stats)]


class MGSIIndex(GlobalRangeIndex):
    def __init__(self, axes=("lon", "lat")):
        self._init_global_index(axes=axes)
        self.local_hash = {}
        self.memory_kdtrees = {}
