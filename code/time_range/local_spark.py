"""time_range/local-spark.py 本地版 —— 复现自 D:\files\mgsi-code\time_range\local-spark.py

注意：原脚本名为 local-spark 但内部是 "DegradedSpark"——MGSI 三级索引跑在 Spark 上的降级版
（binaryFiles 读 HDFS → 分块 → Driver 端建索引 → Spark SQL 查询块均值）。
改动：sc.binaryFiles(hdfs://...) 改为 sc.parallelize(本地路径).map(读字节)+flatMap(分块)，
逻辑、内存查询计时结构与原脚本一致。

用法:
    python local_spark.py [清单名]   # 默认 merra2_one_day
"""
import os
import sys
import time

import numpy as np
import io
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import udf
from scipy.spatial import KDTree

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from mgsi_common import dataio
from mgsi_common.config import COLORADO_BBOX, CHUNK_SIZE, UNIT_SCALE
from mgsi_common.runtime import Recorder


# 本地三级索引结构（与原脚本一致）
class MGSIIndex:
    def __init__(self):
        self.global_kdtree = None
        self.global_points = []
        self.global_metadata = []
        self.local_hash = {}
        self.memory_kdtrees = {}

    def build_global_index(self, chunks_metadata):
        """构建全局k-d树索引"""
        points = []
        self.global_metadata = []
        for meta in chunks_metadata:
            rep_point = (meta['lon_min'], meta['lat_min'])
            points.append(rep_point)
            self.global_metadata.append(meta)
        print(f"[全局索引] 构建索引，包含{len(points)}个分块代表点")
        self.global_kdtree = KDTree(np.array(points))

    def query_global_index(self, bbox):
        """全局索引范围查询"""
        query_range = [(bbox['lon_min'], bbox['lat_min']),
                       (bbox['lon_max'], bbox['lat_max'])]
        center = np.mean(query_range, axis=0)

        dx = bbox['lon_max'] - bbox['lon_min']
        dy = bbox['lat_max'] - bbox['lat_min']
        radius = np.sqrt((dx / 2) ** 2 + (dy / 2) ** 2) + 5.0  # 增加5度缓冲

        print(f"[全局查询] 中心点：{center}，半径：{radius:.2f} 度")
        start_time = time.time()
        indices = self.global_kdtree.query_ball_point(center, radius)
        latency = time.time() - start_time
        print(f"[全局查询] KDTree查询耗时：{latency * 1000:.2f}ms，匹配到{len(indices)}个分块")
        return [self.global_metadata[i] for i in indices]


# 初始化索引
mgsi_index = MGSIIndex()


def read_file_bytes(file_path):
    """Executor 端读本地文件字节（替代 binaryFiles 的 (path, content) 对）"""
    with open(file_path, 'rb') as f:
        return (file_path, f.read())


def load_and_chunk(file_path_content):
    """处理二进制文件内容进行分块（与原脚本一致）"""
    file_path, content = file_path_content
    chunk_size = CHUNK_SIZE
    try:
        with io.BytesIO(content) as file_data:
            with h5py.File(file_data, 'r') as data:
                lats = data['lat'][:]
                lons = data['lon'][:]
                precip = data['PRECTOT']

                lat_size = precip.shape[1]
                lon_size = precip.shape[2]
                lat_step = lat_size // chunk_size
                lon_step = lon_size // chunk_size

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
        print(f"[错误] 处理文件 {file_path} 失败：{e}")
        return []


def build_local_index(chunks_meta):
    """构建局部哈希索引（Driver端执行）"""
    start_time = time.time()
    for meta in chunks_meta:
        mgsi_index.local_hash[meta['chunk_id']] = (
            {k: meta[k] for k in ['lat_min', 'lat_max', 'lon_min', 'lon_max']},
            {
                'file_path': meta['file_path'],
                'lat_range': meta['lat_range'],
                'lon_range': meta['lon_range']
            }
        )
    print(f"[耗时计算-子模块] 构建{len(chunks_meta)}个分块索引，耗时：{(time.time() - start_time) * 1000:.2f}ms")


def load_data_to_memory(chunk_id):
    start_time = time.time()

    """加载数据到内存（Driver端执行）"""
    if chunk_id in mgsi_index.memory_kdtrees:
        return

    logical_meta, physical_meta = mgsi_index.local_hash[chunk_id]

    print(f"[内存加载] 正在加载分块 {chunk_id}")
    try:
        with dataio.open_h5py(physical_meta['file_path']) as data:
            precip = data['PRECTOT']
            lats = data['lat'][:]
            lons = data['lon'][:]

            lat_start, lat_end = physical_meta['lat_range']
            lon_start, lon_end = physical_meta['lon_range']

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
        print(f"[耗时计算-子模块] 分块 {chunk_id} 加载完成，耗时：{(time.time() - start_time) * 1000:.2f}ms")
    except Exception as e:
        print(f"[错误] 加载分块 {chunk_id} 失败：{e}")


import h5py


def optimized_spark_job(manifest='merra2_one_day'):
    rec = Recorder('time_range', 'spark', extra={'manifest': manifest})
    start_time = time.time()

    # 初始化Spark环境
    spark = SparkSession.builder \
        .appName("DegradedSpark") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.master", "local[1]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer") \
        .getOrCreate()

    sc = spark.sparkContext

    # 数据清单（替代 binaryFiles(hdfs://...)）
    files = dataio.list_files(manifest, suffix='.nc4')

    # 并行读取和处理文件 + 构建本地索引
    files_rdd = sc.parallelize(files, 1) \
        .repartition(1) \
        .map(read_file_bytes) \
        .flatMap(load_and_chunk)
    all_chunks = files_rdd.collect()

    build_local_index(all_chunks)
    rec.mark('load_and_local_index')
    print(f"[耗时计算] 并行读取和处理文件+构建本地索引：{rec.marks['load_and_local_index'] * 1000:.2f}ms")

    # 构建全局索引
    mgsi_index.build_global_index(all_chunks)
    rec.mark('build_global_index')
    print(f"[耗时计算] 构建全局索引：{rec.marks['build_global_index'] * 1000:.2f}ms")

    # 示例查询
    query_bbox = COLORADO_BBOX

    # 执行全局查询
    print(f"[主流程] 执行空间查询：{query_bbox}")

    query_start_time = time.time()

    target_chunks = mgsi_index.query_global_index(query_bbox)
    rec.mark('global_query')
    print(f"[耗时计算] 全局索引查询：{rec.marks['global_query'] * 1000:.2f}ms")

    # 加载目标分块
    for chunk in target_chunks:
        load_data_to_memory(chunk['chunk_id'])
    rec.mark('load_chunks')
    print(f"[耗时计算] 加载全部目标分块：{rec.marks['load_chunks'] * 1000:.2f}ms")

    # 创建DataFrame
    data = []
    for chunk_id in mgsi_index.memory_kdtrees:
        precip_data, _ = mgsi_index.memory_kdtrees[chunk_id]
        avg_value = float(np.mean(precip_data) * UNIT_SCALE)  # 计算平均值
        data.append((chunk_id, avg_value))

    schema = StructType([
        StructField("chunk_id", StringType()),
        StructField("avg_value", FloatType())
    ])
    df = spark.createDataFrame(data, schema=schema)

    # 注册UDF
    def enhanced_query(lon, lat, precip):
        spatial_avg = np.mean(precip) * UNIT_SCALE
        return f"Enhanced Spatial Avg: {spatial_avg:.4f} mm"
    enhanced_udf = udf(enhanced_query, StringType())
    spark.udf.register("enhanced_query", enhanced_udf)

    rec.mark('create_df_udf')
    print(f"[耗时计算] 创建DataFrame&注册UDF：{rec.marks['create_df_udf'] * 1000:.2f}ms")

    # 内存查询（原计时：sql 构造即停表）
    df.createOrReplaceTempView("precip_data")
    result_df = spark.sql(
        "SELECT chunk_id, enhanced_query(0, 0, avg_value) AS result FROM precip_data")
    rec.mark('memory_query')
    print(f"[耗时计算] 内存查询：{rec.marks['memory_query'] * 1000:.2f}ms")
    total_s = time.time() - query_start_time
    print(f"[实验结果] 总查询时间：{total_s * 1000:.2f}ms")
    rec.mark_total('total_query_s', total_s)

    # 显示结果（show 触发真实执行——修正计时参考）
    print("查询结果：")
    result_df.show(truncate=False)

    spark.stop()
    rec.mark_total('e2e_s', time.time() - start_time)
    rec.finish()


if __name__ == "__main__":
    manifest = sys.argv[1] if len(sys.argv) > 1 else 'merra2_one_day'
    start_time = time.time()
    optimized_spark_job(manifest)
    print(f"[耗时计算] 端到端总耗时：{(time.time() - start_time) * 1000:.2f}ms")
    dataio.close_all()
