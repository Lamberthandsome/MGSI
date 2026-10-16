r"""nearest/local-mgsi.py 本地版 —— 复现自 D:\files\mgsi-code\nearest\local-mgsi.py

k-NN 查询（k=10）：全局KD-tree取最近10块 → 各块内存KD-tree取10候选 → 汇总排序取前10。
改动仅限数据入口（HDFS→本地清单）。

用法:
    python local_mgsi.py [清单名]   # 默认 merra2_one_month
"""
import os
import sys
import time

import numpy as np
import h5py
from pyflink.table import EnvironmentSettings, BatchTableEnvironment, DataTypes
from pyflink.table.udf import udf
from scipy.spatial import KDTree

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from mgsi_common import dataio
from mgsi_common.config import CHUNK_SIZE, QUERY_POINT, KNN_K
from mgsi_common.runtime import Recorder


class MGSIIndex:
    def __init__(self):
        self.global_kdtree = None
        self.global_points = []
        self.global_metadata = []
        self.local_hash = {}
        self.memory_kdtrees = {}  # 存储格式: {chunk_id: (precip_data, kd_tree, points)}

    def build_global_index(self, chunks_metadata):
        """构建全局k-d树索引"""
        points = []
        self.global_metadata = []
        for meta in chunks_metadata:
            rep_lon = (meta['lon_min'] + meta['lon_max']) / 2
            rep_lat = (meta['lat_min'] + meta['lat_max']) / 2
            points.append((rep_lon, rep_lat))
            self.global_metadata.append(meta)
        print(f"[全局索引] 构建索引，包含{len(points)}个分块代表点")
        self.global_kdtree = KDTree(np.array(points))

    def query_global_index(self, point, k=KNN_K):
        """全局索引查询最近的k个分块"""
        print(f"[全局查询] 查询点：{point}，寻找最近的{k}个分块")
        start_time = time.time()
        if k == 1:
            distance, index = self.global_kdtree.query(point, k=1)
            indices = [index]
        else:
            distances, indices = self.global_kdtree.query(point, k=k)
        latency = time.time() - start_time
        print(f"[全局查询] KDTree查询耗时：{latency * 1000:.2f}ms，找到{len(indices)}个分块")
        return [self.global_metadata[i] for i in indices] if k > 1 else [self.global_metadata[indices]]


mgsi_index = MGSIIndex()


def load_and_chunk(file_path, chunk_size=CHUNK_SIZE):
    """加载NetCDF文件并进行逻辑分块"""
    with dataio.open_h5py(file_path) as data:
        lats = data['lat'][:]
        lons = data['lon'][:]
        precip = data['PRECTOT']
        time_size, lat_size, lon_size = precip.shape
        lat_step = lat_size // chunk_size
        lon_step = lon_size // chunk_size
        chunks_meta = []
        for i in range(chunk_size):
            for j in range(chunk_size):
                lat_start = i * lat_step
                lat_end = (i + 1) * lat_step
                lon_start = j * lon_step
                lon_end = (j + 1) * lon_step
                chunk_meta = {
                    'chunk_id': f"{os.path.basename(file_path)}_{i}_{j}",
                    'lat_min': lats[lat_start],
                    'lat_max': lats[lat_end - 1],
                    'lon_min': lons[lon_start],
                    'lon_max': lons[lon_end - 1],
                    'file_path': file_path,
                    'lat_range': (lat_start, lat_end),
                    'lon_range': (lon_start, lon_end)
                }
                chunks_meta.append(chunk_meta)
        return chunks_meta


def build_local_index(chunks_meta):
    for meta in chunks_meta:
        mgsi_index.local_hash[meta['chunk_id']] = (
            {k: meta[k] for k in ['lat_min', 'lat_max', 'lon_min', 'lon_max']},
            {'file_path': meta['file_path'], 'lat_range': meta['lat_range'], 'lon_range': meta['lon_range']}
        )


def load_data_to_memory(chunk_id):
    if chunk_id in mgsi_index.memory_kdtrees:
        return
    logical_meta, physical_meta = mgsi_index.local_hash[chunk_id]
    with dataio.open_h5py(physical_meta['file_path']) as data:
        precip = data['PRECTOT']
        lats = data['lat'][:]
        lons = data['lon'][:]
        lat_start, lat_end = physical_meta['lat_range']
        lon_start, lon_end = physical_meta['lon_range']
        chunk_precip = precip[:, lat_start:lat_end, lon_start:lon_end]
        lon_grid, lat_grid = np.meshgrid(lons[lon_start:lon_end], lats[lat_start:lat_end])
        lat_indices = np.arange(lat_start, lat_end)
        lon_indices = np.arange(lon_start, lon_end)
        lon_grid_idx, lat_grid_idx = np.meshgrid(lon_indices, lat_indices)
        points = np.column_stack((
            lon_grid.ravel(),
            lat_grid.ravel(),
            lat_grid_idx.ravel(),
            lon_grid_idx.ravel()
        ))
        mgsi_index.memory_kdtrees[chunk_id] = (
            chunk_precip,
            KDTree(points[:, :2]),
            points
        )


@udf(input_types=[DataTypes.FLOAT(), DataTypes.FLOAT(), DataTypes.FLOAT()],
     result_type=DataTypes.STRING())
def enhanced_query_udf(lon, lat, precip):
    return f"坐标({lon:.2f}, {lat:.2f}) 平均降水: {precip:.4f} mm"


def optimized_flink_job(manifest='merra2_one_month'):
    rec = Recorder('nearest', 'mgsi', extra={'manifest': manifest, 'k': KNN_K})
    start_time = time.time()

    env_settings = EnvironmentSettings.new_instance().use_blink_planner().in_batch_mode().build()
    t_env = BatchTableEnvironment.create(environment_settings=env_settings)
    t_env.get_config().get_configuration().set_string("taskmanager.memory.process.size", "8192m")
    t_env.get_config().get_configuration().set_string("taskmanager.memory.task.off-heap.size", "1024m")

    # 加载数据并构建索引
    files = dataio.list_files(manifest, suffix='.nc4')
    all_chunks = []
    for f in files:
        chunks_meta = load_and_chunk(f)
        build_local_index(chunks_meta)
        all_chunks.extend(chunks_meta)

    rec.mark('load_and_local_index')
    print(f"[耗时计算] 并行读取和处理文件+构建本地索引：{rec.marks['load_and_local_index'] * 1000:.2f}ms")

    mgsi_index.build_global_index(all_chunks)
    rec.mark('build_global_index')
    print(f"[耗时计算] 构建全局索引：{rec.marks['build_global_index'] * 1000:.2f}ms")

    # 执行查询
    target_point = QUERY_POINT  # 经度，纬度
    target_chunks = mgsi_index.query_global_index(target_point, k=KNN_K)
    rec.mark('global_query')
    print(f"[耗时计算] 全局索引查询：{rec.marks['global_query'] * 1000:.2f}ms")

    # 加载目标分块
    for chunk in target_chunks:
        load_data_to_memory(chunk['chunk_id'])
    rec.mark('load_chunks')
    print(f"[耗时计算] 加载全部目标分块：{rec.marks['load_chunks'] * 1000:.2f}ms")

    # 收集所有候选点
    all_candidates = []
    for chunk_id in mgsi_index.memory_kdtrees:
        precip_data, kd_tree, points = mgsi_index.memory_kdtrees[chunk_id]
        distances, indices = kd_tree.query(target_point, k=KNN_K)
        if np.isscalar(indices):
            indices = [indices]
            distances = [distances]
        for i, idx in enumerate(indices):
            point_info = points[idx]
            lat_start = mgsi_index.local_hash[chunk_id][1]['lat_range'][0]
            lon_start = mgsi_index.local_hash[chunk_id][1]['lon_range'][0]
            lat_idx = int(point_info[2]) - lat_start
            lon_idx = int(point_info[3]) - lon_start
            avg_precip = np.mean(precip_data[:, lat_idx, lon_idx]) * 1000
            all_candidates.append((point_info[0], point_info[1], avg_precip, distances[i]))

    # 按配置筛选最近的 KNN_K 个点
    sorted_candidates = sorted(all_candidates, key=lambda x: x[3])
    nearest_candidates = sorted_candidates[:KNN_K]

    rec.mark('knn_query')
    print(f"[耗时计算] 最近邻查询：{rec.marks['knn_query'] * 1000:.2f}ms")

    # 创建Flink Table
    data = []
    for point in nearest_candidates:
        data.append((float(point[0]), float(point[1]), float(point[2])))

    table = t_env.from_elements(
        data,
        DataTypes.ROW([
            DataTypes.FIELD("lon", DataTypes.FLOAT()),
            DataTypes.FIELD("lat", DataTypes.FLOAT()),
            DataTypes.FIELD("avg_value", DataTypes.FLOAT())
        ])
    )

    t_env.register_function("enhanced_query", enhanced_query_udf)
    result = t_env.sql_query("SELECT enhanced_query(`lon`, `lat`, `avg_value`) AS `result` FROM `%s`" % table)
    rec.mark('create_table_query')
    print(f"[耗时计算] 创建FlinkTable&内存查询：{rec.marks['create_table_query'] * 1000:.2f}ms")

    # 输出结果
    print("查询结果：")
    result.to_pandas().apply(print)

    rec.mark_total('e2e_s', time.time() - start_time)
    rec.finish()


if __name__ == "__main__":
    optimized_flink_job(sys.argv[1] if len(sys.argv) > 1 else 'merra2_one_month')
    dataio.close_all()
