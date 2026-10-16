"""point/local-mgsi.py 本地版 —— 复现自 D:\files\mgsi-code\point\local-mgsi.py

点查询：全局KD-tree(块中心代表点)定位目标点所在分块 → 内存KD-tree最近邻 → Flink Table输出。
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
from mgsi_common.config import CHUNK_SIZE, QUERY_POINT
from mgsi_common.runtime import Recorder


class MGSIIndex:
    def __init__(self):
        self.global_kdtree = None
        self.global_metadata = []
        self.local_hash = {}  # 存储分块元数据
        self.memory_kdtrees = {}  # 存储格式: {chunk_id: (precip_data, kd_tree, points)}

    def build_global_index(self, chunks_metadata):
        """构建全局k-d树索引"""
        points = []
        self.global_metadata = []
        for meta in chunks_metadata:
            # 使用分块中心点作为代表点
            rep_lon = (meta['lon_min'] + meta['lon_max']) / 2
            rep_lat = (meta['lat_min'] + meta['lat_max']) / 2
            points.append((rep_lon, rep_lat))
            self.global_metadata.append(meta)
        print(f"[全局索引] 构建索引，包含{len(points)}个分块代表点")
        self.global_kdtree = KDTree(np.array(points))

    def query_global_index(self, point):
        """全局索引查询目标点所在的分块"""
        print(f"[全局查询] 查询点：{point}")
        start_time = time.time()
        distance, index = self.global_kdtree.query(point, k=1)
        latency = time.time() - start_time
        print(f"[全局查询] KDTree查询耗时：{latency * 1000:.2f}ms，找到目标分块")
        return self.global_metadata[index]

    def load_data_to_memory(self, chunk_id, file_path, lat_range, lon_range):
        """加载指定分块数据并构建本地KD树"""
        if chunk_id in self.memory_kdtrees:
            return

        with dataio.open_h5py(file_path) as data:
            precip = data['PRECTOT']
            lats = data['lat'][:]
            lons = data['lon'][:]
            lat_start, lat_end = lat_range
            lon_start, lon_end = lon_range

            # 确保索引不越界
            actual_lat_end = min(lat_end, lats.shape[0])
            actual_lon_end = min(lon_end, lons.shape[0])

            # 加载分块数据
            chunk_precip = precip[:, lat_start:actual_lat_end, lon_start:actual_lon_end]
            lon_grid, lat_grid = np.meshgrid(lons[lon_start:actual_lon_end], lats[lat_start:actual_lat_end])
            points = np.column_stack((lon_grid.ravel(), lat_grid.ravel()))

            # 存储分块数据和KD树
            self.memory_kdtrees[chunk_id] = (
                chunk_precip,
                KDTree(points),
                points,
                (actual_lat_end - lat_start, actual_lon_end - lon_start)  # 实际分块尺寸
            )

    def query_point_value(self, target_point, chunk_id):
        """查询指定分块中目标点的值"""
        if chunk_id not in self.memory_kdtrees:
            self.load_data_to_memory(chunk_id, *self.local_hash[chunk_id])

        precip_data, kd_tree, points, (lat_size, lon_size) = self.memory_kdtrees[chunk_id]

        # 查询最近邻点
        distance, index = kd_tree.query(target_point, k=1)

        # 计算网格索引
        lat_idx = index // lon_size
        lon_idx = index % lon_size

        # 检查索引是否越界
        if lat_idx >= precip_data.shape[1] or lon_idx >= precip_data.shape[2]:
            raise IndexError(f"索引越界: lat_idx={lat_idx}(max {precip_data.shape[1]-1}), "
                            f"lon_idx={lon_idx}(max {precip_data.shape[2]-1})")

        # 计算平均降水值
        avg_precip = np.nanmean(precip_data[:, lat_idx, lon_idx]) * 1000  # 转换为mm
        return points[index], avg_precip, distance


def load_and_chunk(file_path, chunk_size=CHUNK_SIZE):
    """加载NetCDF文件并进行逻辑分块"""
    with dataio.open_h5py(file_path) as data:
        lats = data['lat'][:]
        lons = data['lon'][:]
        time_size, lat_size, lon_size = data['PRECTOT'].shape

        # 动态计算分块步长
        lat_step = max(1, lat_size // chunk_size)
        lon_step = max(1, lon_size // chunk_size)

        chunks_meta = []
        for i in range(chunk_size):
            for j in range(chunk_size):
                lat_start = i * lat_step
                lat_end = min((i + 1) * lat_step, lat_size)
                lon_start = j * lon_step
                lon_end = min((j + 1) * lon_step, lon_size)

                # 跳过空分块
                if lat_start >= lat_end or lon_start >= lon_end:
                    continue

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


@udf(input_types=[DataTypes.FLOAT(), DataTypes.FLOAT(), DataTypes.FLOAT()],
     result_type=DataTypes.STRING())
def enhanced_query_udf(lon, lat, precip):
    """自定义UDF，格式化查询结果"""
    return f"坐标({lon:.2f}, {lat:.2f}) 平均降水: {precip:.4f} mm"


def optimized_flink_job(manifest='merra2_one_month'):
    rec = Recorder('point', 'mgsi', extra={'manifest': manifest})
    start_time = time.time()

    # 初始化Flink环境
    env_settings = EnvironmentSettings.new_instance().use_blink_planner().in_batch_mode().build()
    t_env = BatchTableEnvironment.create(environment_settings=env_settings)
    t_env.get_config().get_configuration().set_string("taskmanager.memory.process.size", "8192m")
    t_env.get_config().get_configuration().set_string("taskmanager.memory.task.off-heap.size", "1024m")

    # 初始化索引系统
    mgsi_index = MGSIIndex()

    # 加载数据集
    files = dataio.list_files(manifest, suffix='.nc4')
    all_chunks = []
    for f in files:
        chunks_meta = load_and_chunk(f)
        for meta in chunks_meta:
            mgsi_index.local_hash[meta['chunk_id']] = (
                meta['file_path'],
                meta['lat_range'],
                meta['lon_range']
            )
        all_chunks.extend(chunks_meta)
    rec.mark('load_and_local_index')
    print(f"[耗时计算] 并行读取和处理文件+构建本地索引：{rec.marks['load_and_local_index'] * 1000:.2f}ms")

    # 构建全局索引
    mgsi_index.build_global_index(all_chunks)
    rec.mark('build_global_index')
    print(f"[耗时计算] 构建全局索引：{rec.marks['build_global_index'] * 1000:.2f}ms")

    # 查询目标点所在的分块
    target_point = QUERY_POINT  # 目标点（经度，纬度）
    target_chunk = mgsi_index.query_global_index(target_point)
    print(f"目标分块: {target_chunk['chunk_id']}")
    rec.mark('global_query')
    print(f"[耗时计算] 全局索引查询：{rec.marks['global_query'] * 1000:.2f}ms")

    # 查询目标点的值
    nearest_point, avg_precip, distance = mgsi_index.query_point_value(
        target_point, target_chunk['chunk_id']
    )
    print(f"目标点: {target_point}, 最近邻点: {nearest_point}, 平均降水: {avg_precip:.4f} mm, 距离: {distance:.4f}")
    rec.mark('point_query')
    print(f"[耗时计算] 查询目标点的值：{rec.marks['point_query'] * 1000:.2f}ms")

    # 创建Flink Table
    table = t_env.from_elements(
        [(float(nearest_point[0]), float(nearest_point[1]), float(avg_precip))],
        DataTypes.ROW([
            DataTypes.FIELD("lon", DataTypes.FLOAT()),
            DataTypes.FIELD("lat", DataTypes.FLOAT()),
            DataTypes.FIELD("avg_value", DataTypes.FLOAT())
        ])
    )

    # 注册UDF并执行查询
    t_env.register_function("enhanced_query", enhanced_query_udf)
    rec.mark('create_table')
    print(f"[耗时计算] 创建FlinkTable&内存查询：{rec.marks['create_table'] * 1000:.2f}ms")

    result = t_env.sql_query("SELECT enhanced_query(`lon`, `lat`, `avg_value`) AS `result` FROM `%s`" % table)
    rec.mark('memory_query')
    print(f"[耗时计算] 内存查询：{rec.marks['memory_query'] * 1000:.2f}ms")

    # 输出结果
    print("查询结果：")
    result.to_pandas().apply(print)

    rec.mark_total('e2e_s', time.time() - start_time)
    rec.finish()


if __name__ == "__main__":
    optimized_flink_job(sys.argv[1] if len(sys.argv) > 1 else 'merra2_one_month')
    dataio.close_all()
