"""dataset_change/merra2 本地版 —— 复现自 D:\files\mgsi-code\dataset_change\mgsi-merra2.py

科罗拉多区域掩膜 → 逐点进 Flink Table → UDF 查询。改动仅限数据入口。

用法:
    python merra2.py [清单名]   # 默认 merra2_one_day
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


def load_netcdf_file(file_path):
    print(f"Loading file: {file_path}")
    try:
        with dataio.open_h5py(file_path) as data:
            lat = data['lat'][:]
            lon = data['lon'][:]
            precip = data['PRECTOT'][:, :, :]
            print(f"First time step precip data (min, max): {np.min(precip[0])}, {np.max(precip[0])}")
    except Exception as e:
        print(f"Error reading h5py file {file_path}: {e}")
        return None, None, None

    print(f"Loaded data: lat size = {lat.size}, lon size = {lon.size}, precip shape = {precip.shape}")
    return lat, lon, precip


def filter_colorado_region(lat, lon, precip):
    if lat is None or lon is None or precip is None:
        print("Error: lat, lon, or precip data is None, skipping this file.")
        return None, None, None

    lat_min, lat_max = 38.0, 41.0
    lon_min, lon_max = -109.0, -102.0

    lon_grid, lat_grid = np.meshgrid(lon, lat)
    mask = (lat_grid >= lat_min) & (lat_grid <= lat_max) & (lon_grid >= lon_min) & (lon_grid <= lon_max)

    filtered_lat = lat_grid[mask]
    filtered_lon = lon_grid[mask]
    filtered_precip = precip[:, mask]

    print(f"Filtered data: lat size = {filtered_lat.size}, lon size = {filtered_lon.size}, precip shape = {filtered_precip.shape}")
    return filtered_lat, filtered_lon, filtered_precip


def calculate_spatial_average(precip):
    if precip is None:
        return None
    precip_mm = precip * 1000
    return np.mean(precip_mm)


@udf(input_types=[DataTypes.FLOAT(), DataTypes.FLOAT(), DataTypes.FLOAT()], result_type=DataTypes.STRING())
def query_spatial_average_udf(lat, lon, precip):
    spatial_avg = calculate_spatial_average(precip)
    return f"Spatial Average: {spatial_avg:.4f}"


def flink_job(manifest='merra2_one_day'):
    rec = Recorder('dataset_change', 'mgsi-merra2', extra={'manifest': manifest})
    total_query_time = 0
    total_fixed_time = 0
    env_settings = EnvironmentSettings.new_instance().use_blink_planner().in_batch_mode().build()
    t_env = BatchTableEnvironment.create(environment_settings=env_settings)
    t_env.get_config().get_configuration().set_string("taskmanager.memory.task.off-heap.size", "100mb")

    nc4_files = dataio.list_files(manifest, suffix='.nc4')
    if not nc4_files:
        print(f"No .nc4 files found for manifest {manifest}.")
        return

    for file_path in nc4_files:
        lat, lon, precip = load_netcdf_file(file_path)

        if lat is not None and lon is not None and precip is not None:
            lat_f, lon_f, precip_f = filter_colorado_region(lat, lon, precip)

            data = [(float(lat_f[i]), float(lon_f[i]), float(precip_f[:, i].mean())) for i in range(len(lat_f))]
            table = t_env.from_elements(
                data,
                DataTypes.ROW([
                    DataTypes.FIELD("lat", DataTypes.FLOAT()),
                    DataTypes.FIELD("lon", DataTypes.FLOAT()),
                    DataTypes.FIELD("precip", DataTypes.FLOAT())
                ])
            )

            t_env.register_function("spatial_avg_query", query_spatial_average_udf)

            # 执行查询并计时（原计时：到 sql_query 构造为止）
            start_time = time.time()
            result = t_env.sql_query("SELECT `spatial_avg_query`(`lat`, `lon`, `precip`) AS `result` FROM `%s`" % table)
            end_time = time.time()
            query_time = end_time - start_time
            print(f"Query time for file {os.path.basename(file_path)}: {query_time:.4f} seconds")
            total_query_time += query_time

            # 修正计时：触发执行
            fixed_start = time.time()
            pdf = result.to_pandas()
            total_fixed_time += time.time() - fixed_start
            print(f"Results for file: {os.path.basename(file_path)}")
            pdf.head().apply(print)

    print(f"Total query time : {total_query_time:.4f} seconds")
    print(f"Total query time (fixed): {total_fixed_time:.4f} seconds")
    rec.mark_total('query_s', total_query_time)
    rec.mark_total('query_fixed_s', total_fixed_time)
    rec.finish()


if __name__ == "__main__":
    flink_job(sys.argv[1] if len(sys.argv) > 1 else 'merra2_one_day')
    dataio.close_all()
