"""消融新增变体：Remove Dynamic Repartitioning（论文表7第5行，原代码缺失，本文件补齐）

语义：保留三级索引结构，但去掉"按查询命中块动态构造Table"的按需加载/动态调度，
改为静态策略——一次性把全部256×N个分块的均值全量载入并建表，
查询在SQL层用WHERE过滤（对应论文"static range-based strategy"）。

与 mgsi-2-x 无索引全扫的区别：这里索引剪枝仍发生（打印命中数），
但数据加载不做动态调度，全量块进Table。
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
from _ablation_common import load_and_chunk, build_local_index, load_chunk_with_kdtree, QUERY_BBOX


class MGSIIndex(GlobalRangeIndex):
    def __init__(self):
        self._init_global_index()
        self.local_hash = {}
        self.memory_kdtrees = {}




mgsi_index = MGSIIndex()


@udf(input_types=[DataTypes.FLOAT(), DataTypes.FLOAT(), DataTypes.FLOAT()],
     result_type=DataTypes.STRING())
def enhanced_query_udf(lon, lat, precip):
    spatial_avg = np.mean(precip) * 1000
    return f"Enhanced Spatial Avg: {spatial_avg:.4f} mm"


def optimized_flink_job(manifest='merra2_one_day'):
    rec = Recorder('ablation', 'remove_dynamic_repartitioning', extra={'manifest': manifest})
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

    # 索引剪枝仍然发生（定位命中块），但不做动态按需加载：
    target_chunks = mgsi_index.query_global_index(query_bbox)
    target_ids = {c['chunk_id'] for c in target_chunks}

    # 静态策略：全量加载所有分块、全量建表（无动态调度）
    for chunk in all_chunks:
        load_chunk_with_kdtree(mgsi_index.memory_kdtrees, mgsi_index.local_hash, chunk['chunk_id'])

    data = []
    for chunk in all_chunks:
        cid = chunk['chunk_id']
        if cid in mgsi_index.memory_kdtrees:
            precip_data = mgsi_index.memory_kdtrees[cid][0]
            avg_value = float(np.mean(precip_data) * 1000)
            # 静态分区的经纬度列随行携带，靠SQL WHERE过滤（无动态重分区）
            data.append((cid, float(chunk['lon_min']), float(chunk['lat_min']), avg_value))

    table = t_env.from_elements(
        data,
        DataTypes.ROW([
            DataTypes.FIELD("chunk_id", DataTypes.STRING()),
            DataTypes.FIELD("lon_min", DataTypes.FLOAT()),
            DataTypes.FIELD("lat_min", DataTypes.FLOAT()),
            DataTypes.FIELD("avg_value", DataTypes.FLOAT())
        ])
    )
    t_env.register_table("static_chunks", table)
    t_env.register_function("enhanced_query", enhanced_query_udf)

    result = t_env.sql_query(
        "SELECT chunk_id, enhanced_query(0, 0, avg_value) AS `result` FROM static_chunks "
        "WHERE lon_min BETWEEN -180.0 AND -90.0 AND lat_min BETWEEN 30.0 AND 50.0")

    print("查询结果：")
    total_ms = (time.time() - start) * 1000
    print(f"\n总执行时间：{total_ms:.1f}ms（命中{len(target_ids)}块，全量建表{len(data)}块）")
    result.to_pandas().apply(print)

    rec.mark_total('total_ms', total_ms)
    rec.finish()


if __name__ == "__main__":
    optimized_flink_job(sys.argv[1] if len(sys.argv) > 1 else 'merra2_one_day')
    dataio.close_all()
