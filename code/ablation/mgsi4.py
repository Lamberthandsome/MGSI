"""消融变体4：Remove In-Memory Index —— 移植自 D:\files\mgsi-code\ablation\mgsi4.py

内存只存分块数组（memory_data），不构建内存KD-tree；
全局查询也退化为对代表点的线性距离扫描（原mgsi4行为如此，照搬）。
"""
import os
import sys
import time

import numpy as np
import h5py
from pyflink.table import EnvironmentSettings, BatchTableEnvironment, DataTypes
from pyflink.table.udf import udf

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from mgsi_common import dataio
from mgsi_common.runtime import Recorder
from _ablation_common import load_and_chunk, build_local_index, QUERY_BBOX


class MGSIIndex:
    def __init__(self):
        self.global_metadata = []
        self.local_hash = {}
        self.memory_data = {}  # 存储内存中数据（无KD树）

    def build_global_index(self, chunks_metadata):
        """构建全局索引（仅收集元数据）"""
        self.global_metadata = []
        for meta in chunks_metadata:
            self.global_metadata.append(meta)
        print(f"[全局索引] 构建索引，包含{len(self.global_metadata)}个分块代表点")

    def query_global_index(self, bbox):
        """全局索引范围查询（线性距离扫描）"""
        query_range = [(bbox['lon_min'], bbox['lat_min']),
                       (bbox['lon_max'], bbox['lat_max'])]
        center = np.mean(query_range, axis=0)

        dx = bbox['lon_max'] - bbox['lon_min']
        dy = bbox['lat_max'] - bbox['lat_min']
        radius = np.sqrt((dx / 2) ** 2 + (dy / 2) ** 2)

        buffer = 5.0
        radius += buffer

        print(f"[全局查询] 中心点：{center}，半径：{radius:.2f} 度")
        start_time = time.time()

        matched_chunks = []
        for meta in self.global_metadata:
            dist = np.sqrt((center[0] - meta['lon_min']) ** 2 + (center[1] - meta['lat_min']) ** 2)
            if dist <= radius:
                matched_chunks.append(meta)

        latency = time.time() - start_time
        print(f"[全局查询] 查询耗时：{latency * 1000:.2f}ms，匹配到{len(matched_chunks)}个分块")
        return matched_chunks


mgsi_index = MGSIIndex()


def load_data_to_memory(chunk_id):
    """加载数据到内存并存储数据（无KD树）"""
    if chunk_id in mgsi_index.memory_data:
        return

    logical_meta, physical_meta = mgsi_index.local_hash[chunk_id]

    print(f"[内存加载] 正在加载分块 {chunk_id}")
    start = time.time()
    try:
        with dataio.open_h5py(physical_meta['file_path']) as data:
            precip = data['PRECTOT']
            lat_start, lat_end = physical_meta['lat_range']
            lon_start, lon_end = physical_meta['lon_range']

            chunk_precip = precip[:, lat_start:lat_end, lon_start:lon_end]
            mgsi_index.memory_data[chunk_id] = chunk_precip

        print(f"[内存加载] 分块 {chunk_id} 加载完成，耗时：{(time.time() - start) * 1000:.2f}ms")
    except Exception as e:
        print(f"[错误] 加载分块 {chunk_id} 失败：{e}")


@udf(input_types=[DataTypes.FLOAT(), DataTypes.FLOAT(), DataTypes.FLOAT()],
     result_type=DataTypes.STRING())
def enhanced_query_udf(lon, lat, precip):
    spatial_avg = np.mean(precip) * 1000
    return f"Enhanced Spatial Avg: {spatial_avg:.4f} mm"


def optimized_flink_job(manifest='merra2_one_day'):
    rec = Recorder('ablation', 'remove_in_memory_index', extra={'manifest': manifest})
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
        build_local_index(mgsi_index.local_hash, chunks_meta)
        all_chunks.extend(chunks_meta)

    print(f"[耗时计算] 并行读取和处理文件+构建本地索引：{(time.time() - start_time) * 1000:.2f}ms")

    mgsi_index.build_global_index(all_chunks)

    start = time.time()
    query_bbox = QUERY_BBOX

    query_start_time = time.time()

    target_chunks = mgsi_index.query_global_index(query_bbox)

    print(f"[主流程] 需要加载{len(target_chunks)}个分块到内存")
    for chunk in target_chunks:
        load_data_to_memory(chunk['chunk_id'])

    data = []
    for chunk_id in mgsi_index.memory_data:
        precip_data = mgsi_index.memory_data[chunk_id]
        avg_value = float(np.mean(precip_data) * 1000)
        data.append((chunk_id, avg_value))

    table = t_env.from_elements(
        data,
        DataTypes.ROW([DataTypes.FIELD("chunk_id", DataTypes.STRING()),
                       DataTypes.FIELD("avg_value", DataTypes.FLOAT())])
    )

    t_env.register_function("enhanced_query", enhanced_query_udf)

    result = t_env.sql_query(
        "SELECT `chunk_id`, enhanced_query(0, 0, `avg_value`) AS `result` FROM `%s`" % table)
    print(f"[实验结果] 总查询时间：{(time.time() - query_start_time) * 1000:.2f}ms")

    print("查询结果：")
    total_ms = (time.time() - start) * 1000
    print(f"\n总执行时间：{total_ms:.1f}ms")
    result.to_pandas().apply(print)

    rec.mark_total('total_ms', total_ms)
    rec.finish()


if __name__ == "__main__":
    optimized_flink_job(sys.argv[1] if len(sys.argv) > 1 else 'merra2_one_day')
    dataio.close_all()
