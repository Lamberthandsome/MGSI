"""消融变体2：Remove Global Index —— 移植自 D:\files\mgsi-code\ablation\mgsi2.py

保留16×16逻辑分块+local_hash，但全局查询不用KD-tree，
改为线性遍历全部分块元数据做bbox相交过滤。
（原文件末尾粘贴的裸数字结果已被删除——那是导致SyntaxError的bug）

注意：mgsi2原始代码中内存加载后直接把块均值放进memory_kdtrees键下（无KD树），
本移植保持该行为。
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
from _ablation_common import load_and_chunk, build_local_index, load_chunk_with_kdtree, QUERY_BBOX


class MGSIIndex:
    def __init__(self):
        self.local_hash = {}
        self.memory_kdtrees = {}


mgsi_index = MGSIIndex()


def load_data_to_memory(chunk_id):
    """加载分块数据到内存（无KD树构建开销计入均值流）"""
    load_chunk_with_kdtree(mgsi_index.memory_kdtrees, mgsi_index.local_hash, chunk_id)


@udf(input_types=[DataTypes.FLOAT(), DataTypes.FLOAT(), DataTypes.FLOAT()],
     result_type=DataTypes.STRING())
def enhanced_query_udf(lon, lat, precip):
    spatial_avg = np.mean(precip) * 1000
    return f"Enhanced Spatial Avg: {spatial_avg:.4f} mm"


def optimized_flink_job(manifest='merra2_one_day'):
    rec = Recorder('ablation', 'remove_global_index', extra={'manifest': manifest})
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

    print(f"[主流程] 加载数据并构建索引完成，耗时：{(time.time() - start_time) * 1000:.2f}ms")

    start = time.time()

    # 直接遍历所有分块筛选匹配的分块（无全局索引）
    query_bbox = QUERY_BBOX

    target_chunks = [
        chunk for chunk in all_chunks
        if not (chunk['lon_max'] < query_bbox['lon_min'] or chunk['lon_min'] > query_bbox['lon_max'] or
                chunk['lat_max'] < query_bbox['lat_min'] or chunk['lat_min'] > query_bbox['lat_max'])
    ]

    print(f"[主流程] 直接遍历匹配到 {len(target_chunks)} 个分块")
    print(f"[耗时计算] 直接遍历查询耗时：{(time.time() - start_time) * 1000:.2f}ms")

    for chunk in target_chunks:
        load_data_to_memory(chunk['chunk_id'])

    data = []
    for chunk_id in mgsi_index.memory_kdtrees:
        precip_data = mgsi_index.memory_kdtrees[chunk_id][0]
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

    result = t_env.sql_query("SELECT `chunk_id`, enhanced_query(0, 0, `avg_value`) AS `result` FROM `%s`" % table)
    print("查询结果：")
    total_ms = (time.time() - start) * 1000
    print(f"\n总执行时间：{total_ms:.1f}ms")
    result.to_pandas().apply(print)

    rec.mark_total('total_ms', total_ms)
    rec.finish()


if __name__ == "__main__":
    optimized_flink_job(sys.argv[1] if len(sys.argv) > 1 else 'merra2_one_day')
    dataio.close_all()
