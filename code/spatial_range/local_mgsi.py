"""spatial_range/local-mgsi.py 本地版 —— 复现自 D:\files\mgsi-code\spatial_range\local-mgsi.py

按 R1-R8 边界框过滤点 → Flink Table + UDF 查询（原脚本结构，无索引剪枝）。
改动仅限数据入口（HDFS→本地清单），bbox、计时点与原脚本一致。

用法:
    python local_mgsi.py [清单名]   # 默认 merra2_one_day
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


# 加载 NetCDF 文件数据
def load_netcdf_file(file_path):
    try:
        with dataio.open_h5py(file_path) as data:
            lat = data['lat'][:]
            lon = data['lon'][:]
            humidity = data['PRECTOT'][:, :, :]
        return lat, lon, humidity
    except Exception as e:
        print(f"Error reading h5py file {file_path}: {e}")
        return None, None, None


# 数据过滤（根据边界框范围）
def filter_region(lat, lon, humidity, lat_min, lat_max, lon_min, lon_max):
    if lat is None or lon is None or humidity is None:
        print("Error: lat, lon, or humidity data is None, skipping this region.")
        return None, None, None

    # 创建经纬度网格
    lon_grid, lat_grid = np.meshgrid(lon, lat)

    # 过滤数据：掩膜
    mask = (lat_grid >= lat_min) & (lat_grid <= lat_max) & (lon_grid >= lon_min) & (lon_grid <= lon_max)

    # 提取过滤后的经纬度和湿度数据
    filtered_lat = lat_grid[mask]
    filtered_lon = lon_grid[mask]
    filtered_humidity = humidity[:, mask]  # 对所有时间步的数据进行过滤

    return filtered_lat, filtered_lon, filtered_humidity


# 计算空间平均值
def calculate_spatial_average(humidity):
    if humidity is None:
        return None
    return np.mean(humidity)


# 定义 Flink UDF（查询空间平均值）
@udf(input_types=[DataTypes.FLOAT(), DataTypes.FLOAT(), DataTypes.FLOAT()], result_type=DataTypes.STRING())
def query_spatial_average_udf(lat, lon, humidity):
    spatial_avg = calculate_spatial_average(humidity)
    return f"Spatial Average: {spatial_avg:.4f}"


# Flink 作业
def flink_job(manifest='merra2_one_day'):
    rec = Recorder('spatial_range', 'mgsi', extra={'manifest': manifest})
    total_query_time = 0
    # 初始化环境
    env_settings = EnvironmentSettings.new_instance().use_blink_planner().in_batch_mode().build()
    t_env = BatchTableEnvironment.create(environment_settings=env_settings)

    # 动态设置 TaskManager 的堆外内存
    t_env.get_config().get_configuration().set_string("taskmanager.memory.process.size", "8192m")
    t_env.get_config().get_configuration().set_string("taskmanager.memory.task.off-heap.size", "1024m")

    # 定义空间范围边界框列表（原脚本）
    bboxes = [
        ((-45, -90), (0, 90)),
        ((-45, -90), (45, 90)),
        ((-45, -90), (90, 90)),
        ((-90, -90), (90, 90)),
        ((-90, -180), (45, 120)),
        ((-90, -180), (45, 180)),
        ((-90, -180), (90, 120)),
        ((-90, -180), (90, 180)),
    ]

    nc4_files = dataio.list_files(manifest, suffix='.nc4')
    if not nc4_files:
        print(f"No .nc4 files found for manifest {manifest}.")
        return

    times = [0, 0, 0, 0, 0, 0, 0, 0]
    # 遍历所有 .nc4 文件并处理
    for file_path in nc4_files:
        lat, lon, humidity = load_netcdf_file(file_path)

        if lat is None or lon is None or humidity is None:
            continue

        # 遍历每个空间范围
        for bbox_idx, bbox in enumerate(bboxes):
            start_time = time.time()
            (lat_min, lon_min), (lat_max, lon_max) = bbox
            print(f"Processing bbox {bbox_idx + 1}: {lat_min},{lon_min} to {lat_max},{lon_max}")

            # 过滤数据
            lat_f, lon_f, humidity_f = filter_region(lat, lon, humidity, lat_min, lat_max, lon_min, lon_max)
            if lat_f is None or lon_f is None or humidity_f is None:
                print(f"Skipping bbox {bbox_idx + 1} due to no data.")
                continue

            # 转换为 Flink Table
            try:
                data = [(float(lat_f[i]), float(lon_f[i]), float(humidity_f[:, i].mean())) for i in range(len(lat_f))]
            except IndexError as e:
                print(f"Error converting data for Flink Table: {e}")
                continue

            if not data:
                print(f"No data points in bbox {bbox_idx + 1}, skipping query.")
                continue

            table = t_env.from_elements(
                data,
                DataTypes.ROW([
                    DataTypes.FIELD("lat", DataTypes.FLOAT()),
                    DataTypes.FIELD("lon", DataTypes.FLOAT()),
                    DataTypes.FIELD("humidity", DataTypes.FLOAT())
                ])
            )

            # 注册 UDF
            t_env.register_function("spatial_avg_query", query_spatial_average_udf)

            # 执行查询并计时（原计时：到 sql_query 构造为止）
            result = t_env.sql_query(f"SELECT spatial_avg_query(`lat`, `lon`, `humidity`) AS `result` FROM `{table}`")
            end_time = time.time()
            query_time = end_time - start_time
            total_query_time += query_time

            times[bbox_idx] += query_time

    print(times)
    for i, t in enumerate(times):
        rec.mark_total(f'R{i+1}_s', t)
    rec.finish()


# 主程序入口
if __name__ == "__main__":
    flink_job(sys.argv[1] if len(sys.argv) > 1 else 'merra2_one_day')
    dataio.close_all()
