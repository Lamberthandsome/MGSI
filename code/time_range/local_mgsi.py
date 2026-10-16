"""time_range/local-mgsi.py 本地版 —— 复现自 D:\files\mgsi-code\time_range\local-mgsi.py

MGSI 三级索引 + Flink 批处理。输入使用本地清单；全局范围索引采用正文
明确的逐层质心构树与完整块子树剪枝。补充修正计时列（execute 后停表）。

用法:
    python local_mgsi.py [清单名]   # 默认 merra2_one_month（原脚本读 one_month）
"""
import os
import sys
import time

import numpy as np
from pyflink.table import EnvironmentSettings, BatchTableEnvironment, DataTypes
from pyflink.table.udf import udf
from scipy.spatial import KDTree

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from mgsi_common import dataio
from mgsi_common.config import COLORADO_BBOX, CHUNK_SIZE, UNIT_SCALE
from mgsi_common.index import MGSIIndex
from mgsi_common.runtime import Recorder

# 初始化索引
mgsi_index = MGSIIndex()


def load_and_chunk(file_path, chunk_size=CHUNK_SIZE):
    """加载NetCDF文件并进行逻辑分块"""
    print(f"[分块处理] 开始处理文件：{file_path}")
    try:
        with dataio.open_h5py(file_path) as data:
            # 读取经纬度和降水数据
            lats = data['lat'][:]
            lons = data['lon'][:]
            precip = data['PRECTOT']

            # 获取降水数据的维度信息
            time_size = precip.shape[0]  # 时间维度长度
            lat_size = precip.shape[1]  # 纬度维度长度
            lon_size = precip.shape[2]  # 经度维度长度

            # 计算每个分块的纬度、经度步长
            lat_step = lat_size // chunk_size
            lon_step = lon_size // chunk_size

            chunks_meta = []
            for i in range(chunk_size):
                for j in range(chunk_size):
                    # 计算分块的经纬度范围
                    lat_start = i * lat_step
                    lat_end = (i + 1) * lat_step
                    lon_start = j * lon_step
                    lon_end = (j + 1) * lon_step

                    # 生成分块元数据（不再包含 byte_offset 和 byte_length）
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
                'lon_range': meta['lon_range']
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
        with dataio.open_h5py(physical_meta['file_path']) as data:
            # 读取原始数据集
            precip = data['PRECTOT']
            lats = data['lat'][:]
            lons = data['lon'][:]

            # 根据逻辑分块索引提取数据
            lat_start, lat_end = physical_meta['lat_range']
            lon_start, lon_end = physical_meta['lon_range']

            # 提取分块数据
            chunk_precip = precip[:, lat_start:lat_end, lon_start:lon_end]

            # 生成坐标网格
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
    spatial_avg = np.mean(precip) * UNIT_SCALE
    return f"Enhanced Spatial Avg: {spatial_avg:.4f} mm"


def optimized_flink_job(manifest='merra2_one_month'):
    rec = Recorder('time_range', 'mgsi', extra={'manifest': manifest})
    start_time = time.time()

    # 初始化单节点环境
    env_settings = EnvironmentSettings.new_instance().use_blink_planner().in_batch_mode().build()
    t_env = BatchTableEnvironment.create(environment_settings=env_settings)
    t_env.get_config().get_configuration().set_string("taskmanager.memory.process.size", "8192m")
    t_env.get_config().get_configuration().set_string("taskmanager.memory.task.off-heap.size", "1024m")

    # 加载数据并构建索引
    files = dataio.list_files(manifest, suffix='.nc4')
    print(f"[主流程] 发现{len(files)}个数据文件")

    all_chunks = []
    for f in files:
        chunks_meta = load_and_chunk(f)
        build_local_index(chunks_meta)
        all_chunks.extend(chunks_meta)

    rec.mark('load_and_local_index')
    print(f"[耗时计算] 并行读取和处理文件+构建本地索引：{rec.marks['load_and_local_index'] * 1000:.2f}ms")

    # 构建全局索引
    print(f"[主流程] 开始构建全局索引，共{len(all_chunks)}个分块")
    mgsi_index.build_global_index(all_chunks)
    rec.mark('build_global_index')
    print(f"[耗时计算] 构建全局索引：{rec.marks['build_global_index'] * 1000:.2f}ms")

    # 示例查询（科罗拉多区域）
    query_bbox = COLORADO_BBOX

    # 全局索引查询
    print(f"[主流程] 执行空间查询：{query_bbox}")

    query_start_time = time.time()

    target_chunks = mgsi_index.query_global_index(query_bbox)
    rec.mark('global_query')
    print(f"[耗时计算] 全局索引查询：{rec.marks['global_query'] * 1000:.2f}ms")

    # 加载目标数据到内存
    print(f"[主流程] 需要加载{len(target_chunks)}个分块到内存")
    for chunk in target_chunks:
        load_data_to_memory(chunk['chunk_id'])
    rec.mark('load_chunks')
    print(f"[耗时计算] 加载全部目标分块：{rec.marks['load_chunks'] * 1000:.2f}ms")

    # 创建Flink Table
    data = []
    for chunk_id in mgsi_index.memory_kdtrees:
        precip_data, _ = mgsi_index.memory_kdtrees[chunk_id]
        avg_value = float(np.mean(precip_data) * UNIT_SCALE)  # 将 numpy.float64 转换为 Python float
        data.append((chunk_id, avg_value))

    table = t_env.from_elements(
        data,
        DataTypes.ROW([
            DataTypes.FIELD("chunk_id", DataTypes.STRING()),
            DataTypes.FIELD("avg_value", DataTypes.FLOAT())
        ])
    )

    # 注册UDF并执行查询
    t_env.register_function("enhanced_query", enhanced_query_udf)
    rec.mark('create_table_udf')
    print(f"[耗时计算] 创建FlinkTable&注册UDF：{rec.marks['create_table_udf'] * 1000:.2f}ms")

    result = t_env.sql_query(
        "SELECT `chunk_id`, enhanced_query(0, 0, `avg_value`) AS `result` FROM `%s`" % table)
    print(f"[实验结果] 总查询时间：{(time.time() - query_start_time) * 1000:.2f}ms")
    rec.mark('memory_query')
    print(f"[耗时计算] 内存查询：{rec.marks['memory_query'] * 1000:.2f}ms")

    # 输出结果（原脚本用 to_pandas 触发执行）
    print("查询结果：")
    result.to_pandas().apply(print)

    total_s = time.time() - query_start_time
    rec.mark_total('total_query_s', total_s)
    rec.mark_total('e2e_s', time.time() - start_time + rec.marks['load_and_local_index'] + rec.marks['build_global_index'] + rec.marks['global_query'] + rec.marks['load_chunks'] + rec.marks['create_table_udf'] + rec.marks['memory_query'])
    rec.finish()


if __name__ == "__main__":
    manifest = sys.argv[1] if len(sys.argv) > 1 else 'merra2_one_month'
    optimized_flink_job(manifest)
    dataio.close_all()
