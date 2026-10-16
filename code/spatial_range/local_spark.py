"""spatial_range/local-spark.py 本地版 —— 复现自 D:\files\mgsi-code\spatial_range\local-spark.py

Spark RDD 基线：binaryFiles(已改为 parallelize+本地读).cache() → R1-R8 flatMap 过滤 → collect。
改动仅限数据入口，bbox、计时结构一致。

用法:
    python local_spark.py [清单名]   # 默认 merra2_one_day
"""
import os
import sys
import time

import numpy as np
import io
import h5py
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from mgsi_common import dataio
from mgsi_common.runtime import Recorder


def read_file_bytes(file_path):
    with open(file_path, 'rb') as f:
        return (file_path, f.read())


def load_and_query(file_path_content, bbox):
    """加载文件并查询指定边界框内的平均降水量"""
    file_path, content = file_path_content
    try:
        with io.BytesIO(content) as file_data:
            with h5py.File(file_data, 'r') as data:
                lats = data['lat'][:]
                lons = data['lon'][:]
                precip = data['PRECTOT'][:]  # 获取降水量数据

                # 筛选在边界框内的点
                lat_mask = (lats >= bbox['lat_min']) & (lats <= bbox['lat_max'])
                lon_mask = (lons >= bbox['lon_min']) & (lons <= bbox['lon_max'])

                # 获取符合条件的纬度经度索引
                valid_lat_indices = np.where(lat_mask)[0]
                valid_lon_indices = np.where(lon_mask)[0]

                if len(valid_lat_indices) == 0 or len(valid_lon_indices) == 0:
                    return []  # 如果没有符合条件的点，返回空列表

                # 优化数据索引方式：一次性提取所需区域
                valid_precip = precip[:, valid_lat_indices[:, np.newaxis], valid_lon_indices]

                # 计算每个点的平均降水量（跨时间）
                point_avgs = np.mean(valid_precip, axis=0)

                # 计算总平均降水量
                avg_precip = np.mean(point_avgs) * 1000  # 转换为毫米
                return [(file_path, avg_precip)]
    except Exception as e:
        print(f"[错误] 处理文件 {file_path} 失败：{e}")
        return []


def optimized_spark_job(manifest='merra2_one_day'):
    rec = Recorder('spatial_range', 'spark', extra={'manifest': manifest})
    start_time = time.time()

    # 初始化Spark环境
    spark = SparkSession.builder \
        .appName("OptimizedSparkPrecipQuery") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.master", "local[1]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()

    sc = spark.sparkContext

    # 数据清单（替代 binaryFiles(hdfs://...)）
    files = dataio.list_files(manifest, suffix='.nc4')

    # 预先加载并缓存文件数据
    files_rdd = sc.parallelize(files, 1).map(read_file_bytes).repartition(1).cache()

    # 定义查询的边界框列表（原脚本）
    bboxes = [
        {'lon_min': -90,  'lon_max': 90,   'lat_min': -45, 'lat_max': 0},    # 1
        {'lon_min': -90,  'lon_max': 90,   'lat_min': -45, 'lat_max': 45},   # 2
        {'lon_min': -90,  'lon_max': 90,   'lat_min': -45, 'lat_max': 90},   # 3
        {'lon_min': -180, 'lon_max': 180,  'lat_min': -90, 'lat_max': 90},   # 4
        {'lon_min': -180, 'lon_max': 120,  'lat_min': -90, 'lat_max': 45},   # 5
        {'lon_min': -180, 'lon_max': 180,  'lat_min': -90, 'lat_max': 45},   # 6
        {'lon_min': -180, 'lon_max': 120,  'lat_min': -90, 'lat_max': 90},   # 7
        {'lon_min': -180, 'lon_max': 180,  'lat_min': -90, 'lat_max': 90},   # 8
    ]

    # 初始化一个数组来存储每个查询的耗时
    query_times = [0.0] * len(bboxes)

    # 遍历每个查询边界框
    for idx, bbox in enumerate(bboxes):
        print(f"\n=== 正在处理查询 {idx + 1} ===")
        print(f"边界框：{bbox}")

        # 并行查询
        query_start_time = time.time()
        results_rdd = files_rdd.flatMap(lambda x: load_and_query(x, bbox))

        # 计算全局平均降水量
        results = results_rdd.collect()
        total_sum = sum(avg for _, avg in results)
        total_count = len(results)

        if total_count > 0:
            global_avg = total_sum / total_count
        else:
            global_avg = 0.0

        # 记录当前查询的耗时
        query_time = (time.time() - query_start_time) * 1000
        query_times[idx] = query_time

        print(f"总查询时间：{query_time:.2f}ms")

    # 打印所有查询的耗时
    print("\n=== 所有查询耗时 ===")
    for idx, time_ms in enumerate(query_times):
        print(f"查询 {idx + 1} 耗时：{time_ms:.2f}ms")

    spark.stop()
    print(f"[耗时计算] 端到端总耗时：{(time.time() - start_time) * 1000:.2f}ms")
    for i, t in enumerate(query_times):
        rec.mark_total(f'R{i+1}_s', t / 1000.0)
    rec.finish()


if __name__ == "__main__":
    optimized_spark_job(sys.argv[1] if len(sys.argv) > 1 else 'merra2_one_day')
    dataio.close_all()
