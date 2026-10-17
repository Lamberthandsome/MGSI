"""消融变体3：Remove Local Index —— 移植自 D:\files\mgsi-code\ablation\mgsi3.py

去掉local_hash（本地哈希索引）。命中分块后需要重新解析整个文件头（load_and_chunk）
来找回分块的经纬度范围，再读一次文件提取数据
"TaskManagers 扫描本地文件头来定位数据段"的行为。
全局KD-tree与内存KD-tree保留。
全局范围索引采用正文明确的逐层质心构树与完整块子树剪枝。
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
from mgsi_common.index import GlobalRangeIndex
from mgsi_common.runtime import Recorder
from _ablation_common import load_and_chunk, QUERY_BBOX


class MGSIIndex(GlobalRangeIndex):
    def __init__(self):
        self._init_global_index()
        self.memory_kdtrees = {}  # 移除local_hash属性




mgsi_index = MGSIIndex()


def load_data_to_memory(chunk_id, file_path):
    """根据文件路径重新解析元数据并加载数据到内存（无local_hash）"""
    if chunk_id in mgsi_index.memory_kdtrees:
        return

    print(f"[内存加载] 正在加载分块 {chunk_id}")
    start = time.time()
    try:
        # 重新解析文件获取分块元数据（Remove Local Index 的代价）
        chunks_meta = load_and_chunk(file_path)
        target_meta = None
        for meta in chunks_meta:
            if meta['chunk_id'] == chunk_id:
                target_meta = meta
                break

        if not target_meta:
            print(f"[错误] 未找到分块 {chunk_id} 的元数据")
            return

        lat_start, lat_end = target_meta['lat_range']
        lon_start, lon_end = target_meta['lon_range']

        with dataio.open_h5py(file_path) as data:
            precip = data['PRECTOT']
            lats = data['lat'][:]
            lons = data['lon'][:]

            chunk_precip = precip[:, lat_start:lat_end, lon_start:lon_end]

            lon_grid, lat_grid = np.meshgrid(
                lons[lon_start:lon_end],
                lats[lat_start:lat_end]
            )
            points = np.column_stack((lon_grid.ravel(), lat_grid.ravel()))

            mgsi_index.memory_kdtrees[chunk_id] = (
                chunk_precip,
                KDTree(points)
            )

        print(f"[内存加载] 分块 {chunk_id} 加载完成，耗时：{(time.time() - start) * 1000:.2f}ms")
    except Exception as e:
        print(f"[错误] 加载分块 {chunk_id} 失败：{e}")


@udf(input_types=[DataTypes.FLOAT(), DataTypes.FLOAT(), DataTypes.FLOAT()],
     result_type=DataTypes.STRING())
def enhanced_query_udf(lon, lat, precip):
    spatial_avg = np.mean(precip) * 1000
    return f"Enhanced Spatial Avg: {spatial_avg:.4f} mm"


def optimized_flink_job(manifest='merra2_one_day'):
    rec = Recorder('ablation', 'remove_local_index', extra={'manifest': manifest})
    env_settings = EnvironmentSettings.new_instance().use_blink_planner().in_batch_mode().build()
    t_env = BatchTableEnvironment.create(environment_settings=env_settings)
    t_env.get_config().get_configuration().set_string("taskmanager.memory.process.size", "8192m")
    t_env.get_config().get_configuration().set_string("taskmanager.memory.task.off-heap.size", "1024m")

    start_time = time.time()

    files = dataio.list_files(manifest, suffix='.nc4')
    print(f"[主流程] 发现{len(files)}个数据文件")

    all_chunks = []
    for f in files:
        chunks_meta = load_and_chunk(f)
        all_chunks.extend(chunks_meta)

    print(f"[耗时计算] 文件处理耗时：{(time.time() - start_time)*1000:.2f}ms")
    start_time = time.time()

    # 构建全局索引
    mgsi_index.build_global_index(all_chunks)
    print(f"[耗时计算] 构建全局索引耗时：{(time.time() - start_time)*1000:.2f}ms")

    start = time.time()

    # 示例查询
    query_bbox = QUERY_BBOX

    # 全局索引查询
    target_chunks = mgsi_index.query_global_index(query_bbox)

    # 加载目标数据到内存
    print(f"[主流程] 加载{len(target_chunks)}个分块到内存")
    for chunk in target_chunks:
        load_data_to_memory(chunk['chunk_id'], chunk['file_path'])

    # 创建Flink Table
    data = []
    for chunk_id in mgsi_index.memory_kdtrees:
        precip_data, _ = mgsi_index.memory_kdtrees[chunk_id]
        avg_value = float(np.mean(precip_data) * 1000)
        data.append((chunk_id, avg_value))

    table = t_env.from_elements(
        data,
        DataTypes.ROW([
            DataTypes.FIELD("chunk_id", DataTypes.STRING()),
            DataTypes.FIELD("avg_value", DataTypes.FLOAT())
        ])
    )

    t_env.register_function("enhanced_query", enhanced_query_udf)
    result = t_env.sql_query(
        "SELECT `chunk_id`, enhanced_query(0, 0, `avg_value`) AS `result` FROM `%s`" % table)

    # 输出结果
    print("查询结果：")
    total_ms = (time.time() - start) * 1000
    print(f"\n总执行时间：{total_ms:.1f}ms")
    result.to_pandas().apply(print)

    rec.mark_total('total_ms', total_ms)
    rec.finish()


if __name__ == "__main__":
    optimized_flink_job(sys.argv[1] if len(sys.argv) > 1 else 'merra2_one_day')
    dataio.close_all()
