"""dataset_change/ecmwf 本地版 —— 复现自 D:\files\mgsi-code\dataset_change\mgsi-ecmwf.py

变量 e/latitude/longitude，分100时间步累积求时间均值后进 Flink Table + SQL AVG。
原脚本 load 即用本地路径打开（Dataset(file_path)），本移植保持一致，仅清单化列举。
数据缺失时清单为空即跳过。

用法:
    python ecmwf.py [清单名]   # 默认 ecmwf
"""
import os
import sys
import time

import numpy as np
from pyflink.table import EnvironmentSettings, BatchTableEnvironment, DataTypes
from netCDF4 import Dataset

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from mgsi_common import dataio
from mgsi_common.runtime import Recorder


def load_netcdf_file(file_path):
    print(f"Loading file: {file_path}")
    try:
        with Dataset(file_path, mode='r') as data:
            lat = data.variables['latitude'][:]
            lon = data.variables['longitude'][:]

            precip_var = data.variables['e']
            time_steps = precip_var.shape[0]
            lat_size = lat.size
            lon_size = lon.size

            # 初始化二维数组，用于累加每个时间步的总和（毫米单位）
            sum_ij = np.zeros((lat_size, lon_size), dtype=np.float64)
            count_time = 0

            chunk_size = 100  # 每次读取的时间步数
            for i in range(0, time_steps, chunk_size):
                end = min(i + chunk_size, time_steps)
                current_chunk_size = end - i
                print(f"Reading time steps {i} to {end}...")
                chunk_data = precip_var[i:end, :, :] * 1000
                sum_ij += np.sum(chunk_data, axis=0)
                count_time += current_chunk_size

            time_avg = sum_ij / count_time if count_time > 0 else sum_ij
            print(f"Time-averaged precip (min, max): {np.min(time_avg):.2f}, {np.max(time_avg):.2f} mm")

    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None, None, None

    print(f"Processed data: lat={lat.size}, lon={lon.size}")
    return lat, lon, time_avg


def flink_job(manifest='ecmwf'):
    rec = Recorder('dataset_change', 'mgsi-ecmwf', extra={'manifest': manifest})
    total_query_time = 0
    env_settings = EnvironmentSettings.new_instance().use_blink_planner().in_batch_mode().build()
    t_env = BatchTableEnvironment.create(environment_settings=env_settings)
    t_env.get_config().get_configuration().set_string("taskmanager.memory.process.size", "8192m")
    t_env.get_config().get_configuration().set_string("taskmanager.memory.task.off-heap.size", "1024m")

    nc_files = dataio.list_files(manifest, suffix='.nc')
    if not nc_files:
        print(f"No .nc files found for manifest {manifest}（ECMWF 数据待下载，见 docs/DATA_ACQUISITION.md）")
        return

    for file_path in nc_files:
        lat, lon, time_avg = load_netcdf_file(file_path)

        if lat is None or lon is None or time_avg is None:
            continue

        data = [(float(lat[i]), float(lon[j]), float(time_avg[i, j]))
                 for i in range(lat.size) for j in range(lon.size)]
        table = t_env.from_elements(
            data,
            DataTypes.ROW([
                DataTypes.FIELD("latitude", DataTypes.FLOAT()),
                DataTypes.FIELD("longitude", DataTypes.FLOAT()),
                DataTypes.FIELD("avg_precip", DataTypes.FLOAT())
            ])
        )

        # 执行查询（直接使用SQL聚合函数）
        start_time = time.time()
        result = t_env.sql_query(f"""
            SELECT
                AVG(avg_precip) AS spatial_avg,
                COUNT(*) AS point_count
            FROM {table}
        """)
        end_time = time.time()
        query_time = end_time - start_time
        total_query_time += query_time

        result_df = result.to_pandas()
        spatial_avg = result_df.iloc[0]['spatial_avg']
        print(f"文件 {os.path.basename(file_path)} 空间平均降水量: {spatial_avg:.2f} mm")
        print(f"查询耗时: {query_time:.2f}秒")

    print(f"总查询耗时: {total_query_time:.2f}秒")
    rec.mark_total('query_s', total_query_time)
    rec.finish()


if __name__ == "__main__":
    flink_job(sys.argv[1] if len(sys.argv) > 1 else 'ecmwf')
