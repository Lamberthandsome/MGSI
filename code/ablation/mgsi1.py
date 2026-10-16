"""消融变体1：Remove Logical Chunking —— 移植自 D:\files\mgsi-code\ablation\mgsi1.py

整文件为存储单元（HDFS块式），以文件中心点逐层质心构树，完整文件子树边界剪枝，
命中文件整体载入内存后再按bbox掩膜取均值。
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
from mgsi_common.index import GlobalRangeIndex
from mgsi_common.runtime import Recorder
from _ablation_common import QUERY_BBOX


class MGSIIndex(GlobalRangeIndex):
    def __init__(self):
        self._init_global_index(representative="centre")
        self.file_data_cache = {}




mgsi_index = MGSIIndex()


def load_file_metadata(file_path):
    """加载文件元数据"""
    print(f"[文件处理] 处理文件：{file_path}")
    try:
        with dataio.open_h5py(file_path) as data:
            lats = data['lat'][:]
            lons = data['lon'][:]
            return [{
                'file_id': os.path.basename(file_path),
                'lat_min': float(lats.min()),
                'lat_max': float(lats.max()),
                'lon_min': float(lons.min()),
                'lon_max': float(lons.max()),
                'file_path': file_path
            }]
    except Exception as e:
        print(f"[错误] 文件读取失败：{e}")
        return []


def load_file_to_memory(file_id):
    """加载完整文件数据到内存"""
    if file_id in mgsi_index.file_data_cache:
        return

    meta = next((m for m in mgsi_index.global_metadata if m['file_id'] == file_id), None)
    if not meta:
        print(f"[错误] 找不到文件元数据：{file_id}")
        return

    print(f"[内存加载] 加载文件：{file_id}")
    start = time.time()
    try:
        with dataio.open_h5py(meta['file_path']) as data:
            mgsi_index.file_data_cache[file_id] = {
                'precip': data['PRECTOT'][:],
                'lats': data['lat'][:],
                'lons': data['lon'][:]
            }
        print(f"[内存加载] 完成加载，耗时：{(time.time() - start) * 1000:.1f}ms")
    except Exception as e:
        print(f"[错误] 数据加载失败：{e}")


@udf(input_types=[DataTypes.FLOAT(), DataTypes.FLOAT(), DataTypes.FLOAT()],
     result_type=DataTypes.STRING())
def enhanced_query_udf(lon, lat, precip):
    spatial_avg = np.mean(precip) * 1000
    return f"Enhanced Spatial Avg: {spatial_avg:.4f} mm"


def optimized_flink_job(manifest='merra2_one_day'):
    rec = Recorder('ablation', 'remove_logical_chunking', extra={'manifest': manifest})
    env_settings = EnvironmentSettings.new_instance().use_blink_planner().in_batch_mode().build()
    t_env = BatchTableEnvironment.create(environment_settings=env_settings)
    t_env.get_config().get_configuration().set_string("taskmanager.memory.process.size", "256m")
    t_env.get_config().get_configuration().set_string("taskmanager.memory.task.off-heap.size", "128m")

    # 加载文件元数据
    files = dataio.list_files(manifest, suffix='.nc4')
    all_meta = [m for f in files for m in load_file_metadata(f)]
    mgsi_index.build_global_index(all_meta)

    start = time.time()

    # 示例查询
    query_bbox = QUERY_BBOX

    # 执行查询
    target_files = mgsi_index.query_global_index(query_bbox)
    for f in target_files:
        load_file_to_memory(f['file_id'])

    # 创建数据集
    data = []
    for file_meta in target_files:
        file_id = file_meta['file_id']
        cache = mgsi_index.file_data_cache.get(file_id)
        if cache:
            # 提取查询范围内的数据
            lat_mask = (cache['lats'] >= query_bbox['lat_min']) & (cache['lats'] <= query_bbox['lat_max'])
            lon_mask = (cache['lons'] >= query_bbox['lon_min']) & (cache['lons'] <= query_bbox['lon_max'])
            precip_subset = cache['precip'][:, lat_mask, :][:, :, lon_mask]
            avg_value = float(np.mean(precip_subset)) * 1000
            data.append((file_id, avg_value))

    # 注册表并执行查询
    table = t_env.from_elements(
        data,
        DataTypes.ROW([
            DataTypes.FIELD("file_id", DataTypes.STRING()),
            DataTypes.FIELD("avg_value", DataTypes.FLOAT())
        ])
    )
    t_env.register_function("enhanced_query", enhanced_query_udf)
    result = t_env.sql_query("SELECT file_id, enhanced_query(0, 0, avg_value) FROM %s" % table)

    # 输出结果
    print("\n查询结果：")
    total_ms = (time.time() - start) * 1000
    print(f"\n总执行时间：{total_ms:.1f}ms")
    result.to_pandas().apply(print)

    rec.mark_total('total_ms', total_ms)
    rec.finish()


if __name__ == "__main__":
    optimized_flink_job(sys.argv[1] if len(sys.argv) > 1 else 'merra2_one_day')
    dataio.close_all()
