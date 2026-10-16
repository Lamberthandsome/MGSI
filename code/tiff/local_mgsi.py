"""tiff/local-mgsi.py 本地版 —— 复现自 D:\files\mgsi-code\tiff\local-mgsi.py

GeoTIFF（CHIRPS）的 MGSI 管线：rasterio 读取、transform 定位、窗口读取分块。
数据入口使用本地清单；全局范围索引采用正文明确的逐层质心构树与完整块子树剪枝。
数据集缺失时清单为空即跳过。

用法:
    python local_mgsi.py [清单名]   # 默认 chirps
"""
import os
import sys
import time

import numpy as np
import rasterio
from rasterio.transform import Affine
from pyflink.table import EnvironmentSettings, BatchTableEnvironment, DataTypes
from pyflink.table.udf import udf
from scipy.spatial import KDTree

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from mgsi_common import dataio
from mgsi_common.index import MGSIIndex
from mgsi_common.config import COLORADO_BBOX, CHUNK_SIZE
from mgsi_common.runtime import Recorder




mgsi_index = MGSIIndex()


def load_and_chunk(file_path, chunk_size=CHUNK_SIZE):
    """加载GeoTIFF文件并进行逻辑分块"""
    print(f"[分块处理] 开始处理文件：{file_path}")
    try:
        with rasterio.open(file_path) as src:
            transform = src.transform
            width = src.width
            height = src.height
            num_bands = src.count

            lat_step = height // chunk_size
            lon_step = width // chunk_size

            chunks_meta = []
            for i in range(chunk_size):
                for j in range(chunk_size):
                    lat_start = i * lat_step
                    lat_end = min((i + 1) * lat_step, height)
                    lon_start = j * lon_step
                    lon_end = min((j + 1) * lon_step, width)

                    # 计算分块四角的经纬度
                    rows = [lat_start, lat_start, lat_end - 1, lat_end - 1]
                    cols = [lon_start, lon_end - 1, lon_start, lon_end - 1]
                    lons, lats = [], []
                    for row, col in zip(rows, cols):
                        x, y = transform * (col, row)
                        lons.append(x)
                        lats.append(y)

                    chunk_meta = {
                        'chunk_id': f"{os.path.basename(file_path)}_{i}_{j}",
                        'lat_min': min(lats),
                        'lat_max': max(lats),
                        'lon_min': min(lons),
                        'lon_max': max(lons),
                        'file_path': file_path,
                        'lat_range': (lat_start, lat_end),
                        'lon_range': (lon_start, lon_end),
                        'num_bands': num_bands
                    }
                    chunks_meta.append(chunk_meta)
            print(f"[分块处理] 文件{file_path}分割为{len(chunks_meta)}个分块")
            return chunks_meta
    except Exception as e:
        print(f"[错误] 读取文件 {file_path} 失败：{e}")
        return []


def build_local_index(chunks_meta):
    """构建局部哈希索引"""
    start = time.time()
    for meta in chunks_meta:
        mgsi_index.local_hash[meta['chunk_id']] = (
            {k: meta[k] for k in ['lat_min', 'lat_max', 'lon_min', 'lon_max']},
            {
                'file_path': meta['file_path'],
                'lat_range': meta['lat_range'],
                'lon_range': meta['lon_range'],
                'num_bands': meta['num_bands']
            }
        )
    print(f"[局部索引] 构建{len(chunks_meta)}个分块索引，耗时：{(time.time() - start) * 1000:.2f}ms")


def load_data_to_memory(chunk_id):
    """加载数据到内存并构建内存索引"""
    if chunk_id in mgsi_index.memory_kdtrees:
        return

    logical_meta, physical_meta = mgsi_index.local_hash[chunk_id]

    print(f"[内存加载] 正在加载分块 {chunk_id}")
    start = time.time()
    try:
        with rasterio.open(physical_meta['file_path']) as src:
            window = ((physical_meta['lat_range'][0], physical_meta['lat_range'][1]),
                      (physical_meta['lon_range'][0], physical_meta['lon_range'][1]))

            # 读取所有波段数据
            chunk_data = src.read(window=window)

            # 生成坐标网格
            rows = range(window[0][0], window[0][1])
            cols = range(window[1][0], window[1][1])
            rows_grid, cols_grid = np.meshgrid(rows, cols, indexing='ij')
            xs, ys = rasterio.transform.xy(src.transform, rows_grid, cols_grid)
            points = np.column_stack((np.ravel(xs), np.ravel(ys)))

            mgsi_index.memory_kdtrees[chunk_id] = (
                chunk_data,
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


def optimized_flink_job(manifest='chirps'):
    rec = Recorder('tiff', 'mgsi', extra={'manifest': manifest})
    start_time = time.time()

    # 初始化Flink环境
    env_settings = EnvironmentSettings.new_instance().use_blink_planner().in_batch_mode().build()
    t_env = BatchTableEnvironment.create(environment_settings=env_settings)
    t_env.get_config().get_configuration().set_string("taskmanager.memory.process.size", "8192m")
    t_env.get_config().get_configuration().set_string("taskmanager.memory.task.off-heap.size", "100mb")

    files = dataio.list_files(manifest, suffix='.tif')
    print(f"[主流程] 发现{len(files)}个数据文件")
    if not files:
        print("[提示] CHIRPS 数据未下载（见 docs/DATA_ACQUISITION.md），跳过")
        return

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

    query_bbox = COLORADO_BBOX

    query_start_time = time.time()
    target_chunks = mgsi_index.query_global_index(query_bbox)
    rec.mark('global_query')
    print(f"[耗时计算] 全局索引查询：{rec.marks['global_query'] * 1000:.2f}ms")

    for chunk in target_chunks:
        load_data_to_memory(chunk['chunk_id'])
    rec.mark('load_chunks')
    print(f"[耗时计算] 加载全部目标分块：{rec.marks['load_chunks'] * 1000:.2f}ms")

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

    rec.mark('create_table')
    print(f"[耗时计算] 创建FlinkTable&注册UDF：{rec.marks['create_table'] * 1000:.2f}ms")

    t_env.register_function("enhanced_query", enhanced_query_udf)
    result = t_env.sql_query(
        "SELECT `chunk_id`, enhanced_query(0, 0, `avg_value`) AS `result` FROM `%s`" % table)
    print(f"[实验结果] 总查询时间：{(time.time() - query_start_time) * 1000:.2f}ms")
    rec.mark('memory_query')
    print(f"[耗时计算] 内存查询：{rec.marks['memory_query'] * 1000:.2f}ms")

    print("查询结果：")
    result.to_pandas().apply(print)

    rec.mark_total('e2e_s', time.time() - start_time)
    rec.finish()


if __name__ == "__main__":
    optimized_flink_job(sys.argv[1] if len(sys.argv) > 1 else 'chirps')
