"""processor_complete 本地版 —— 复现自 D:\files\mgsi-code\ablation\processor_complete.py

原脚本即纯本地（h5py 直读 + scipy KDTree，无 Flink/HDFS），是 MGSI 三级索引管线的
最简管线。输入使用命令行指定的目录/清单；全局范围索引采用正文明确的
逐层质心构树与完整块子树剪枝。

用法:
    python processor_complete.py <数据目录或清单>   # 默认用 ablation 原配的 19800115.nc4
"""
import os
import sys
import time

import numpy as np
from scipy.spatial import KDTree

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from mgsi_common import dataio
from mgsi_common.index import MGSIIndex

mgsi_index = MGSIIndex()


def load_and_chunk(file_path, chunk_size=16):
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

                    # 生成分块元数据
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


def process_files(files):
    start_time = time.time()

    print(f"[主流程] 发现{len(files)}个数据文件")

    all_chunks = []
    for f in files:
        chunks_meta = load_and_chunk(f)
        build_local_index(chunks_meta)
        all_chunks.extend(chunks_meta)

    print(f"[耗时计算] 读取和处理文件+构建本地索引：{(time.time() - start_time) * 1000:.2f}ms")
    start_time = time.time()

    # 构建全局索引
    print(f"[主流程] 开始构建全局索引，共{len(all_chunks)}个分块")
    mgsi_index.build_global_index(all_chunks)
    print(f"[耗时计算] 构建全局索引：{(time.time() - start_time) * 1000:.2f}ms")
    start_time = time.time()

    # 示例查询（科罗拉多区域）
    from mgsi_common.config import COLORADO_BBOX
    query_bbox = COLORADO_BBOX

    # 全局索引查询
    print(f"[主流程] 执行空间查询：{query_bbox}")
    query_start_time = time.time()

    target_chunks = mgsi_index.query_global_index(query_bbox)
    print(f"[耗时计算] 全局索引查询：{(time.time() - start_time) * 1000:.2f}ms")
    start_time = time.time()

    # 加载目标数据到内存
    print(f"[主流程] 需要加载{len(target_chunks)}个分块到内存")
    for chunk in target_chunks:
        load_data_to_memory(chunk['chunk_id'])
    print(f"[耗时计算] 加载全部目标分块：{(time.time() - start_time) * 1000:.2f}ms")

    # 计算并打印结果
    print("\n查询结果：")
    for chunk_id in mgsi_index.memory_kdtrees:
        precip_data, _ = mgsi_index.memory_kdtrees[chunk_id]
        avg_value = np.mean(precip_data) * 1000  # 转换为mm单位
        print(f"分块 {chunk_id}: 平均降水量 {avg_value:.4f} mm")

    print(f"\n[实验结果] 总查询时间：{(time.time() - query_start_time) * 1000:.2f}ms")
    print(f"[耗时计算] 端到端总耗时：{(time.time() - start_time) * 1000:.2f}ms")


def resolve_files(arg):
    if arg is None:
        return [r'D:\files\mgsi-code\ablation\MERRA2_100.tavg1_2d_flx_Nx.19800115.nc4']
    if os.path.isdir(arg):
        return dataio.list_dir(arg, suffix='.nc4')
    if arg.endswith('.txt'):
        return dataio.read_manifest(arg)
    return [arg]


if __name__ == "__main__":
    files = resolve_files(sys.argv[1] if len(sys.argv) > 1 else None)
    process_files(files)
    dataio.close_all()
