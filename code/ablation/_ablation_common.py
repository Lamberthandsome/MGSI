"""消融实验公共函数 —— 从 D:\files\mgsi-code\ablation\ 各脚本抽取的共享部分（逐行等价）。

各变体差异点：
- mgsi1.py = Remove Logical Chunking（整文件为单元，中心代表点，1.5×缓冲+精确相交）
- mgsi2.py = Remove Global Index（保留分块+local_hash；查询=全分块线性bbox相交扫描）
- mgsi3.py = Remove Local Index（去掉local_hash；加载时重新解析文件头找分块）
- mgsi4.py = Remove In-Memory Index（内存只存数组；全局查询也退化为线性扫描）
- fullscan.py = 无索引全表扫描（mgsi-2-1 移植）
- no_repart.py = Remove Dynamic Repartitioning（新增：静态全量建表+SQL WHERE，无按需加载）
"""
import os
import sys
import time

import numpy as np
import h5py
from scipy.spatial import KDTree

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from mgsi_common import dataio
from mgsi_common.config import CHUNK_SIZE


def load_and_chunk(file_path, chunk_size=CHUNK_SIZE):
    """加载NetCDF文件并进行逻辑分块（与原消融脚本一致）"""
    print(f"[分块处理] 开始处理文件：{file_path}")
    try:
        with dataio.open_h5py(file_path) as data:
            lats = data['lat'][:]
            lons = data['lon'][:]
            precip = data['PRECTOT']

            lat_step = precip.shape[1] // chunk_size
            lon_step = precip.shape[2] // chunk_size

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

            print(f"[分块处理] 文件{file_path}分割为{len(chunks_meta)}个分块")
            return chunks_meta
    except Exception as e:
        print(f"[错误] 读取文件 {file_path} 失败：{e}")
        return []


def build_local_index(local_hash, chunks_meta):
    """构建局部哈希索引（与原脚本一致）"""
    start = time.time()
    for meta in chunks_meta:
        local_hash[meta['chunk_id']] = (
            {k: meta[k] for k in ['lat_min', 'lat_max', 'lon_min', 'lon_max']},
            {
                'file_path': meta['file_path'],
                'lat_range': meta['lat_range'],
                'lon_range': meta['lon_range']
            }
        )
    print(f"[局部索引] 构建{len(chunks_meta)}个分块索引，耗时：{(time.time() - start) * 1000:.2f}ms")


def load_chunk_with_kdtree(memory_kdtrees, local_hash, chunk_id):
    """加载分块数据并构建内存KD树（带local_hash版）"""
    if chunk_id in memory_kdtrees:
        return
    logical_meta, physical_meta = local_hash[chunk_id]
    print(f"[内存加载] 正在加载分块 {chunk_id}")
    start = time.time()
    try:
        with dataio.open_h5py(physical_meta['file_path']) as data:
            precip = data['PRECTOT']
            lats = data['lat'][:]
            lons = data['lon'][:]
            lat_start, lat_end = physical_meta['lat_range']
            lon_start, lon_end = physical_meta['lon_range']
            chunk_precip = precip[:, lat_start:lat_end, lon_start:lon_end]
            lon_grid, lat_grid = np.meshgrid(lons[lon_start:lon_end], lats[lat_start:lat_end])
            points = np.column_stack((lon_grid.ravel(), lat_grid.ravel()))
            memory_kdtrees[chunk_id] = (chunk_precip, KDTree(points))
        print(f"[内存加载] 分块 {chunk_id} 加载完成，耗时：{(time.time() - start) * 1000:.2f}ms")
    except Exception as e:
        print(f"[错误] 加载分块 {chunk_id} 失败：{e}")


QUERY_BBOX = {
    'lon_min': -109.0, 'lon_max': -102.0,
    'lat_min': 38.0, 'lat_max': 41.0,
}
