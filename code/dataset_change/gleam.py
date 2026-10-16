"""dataset_change/gleam 本地版 —— 复现自 D:\files\mgsi-code\dataset_change\mgsi-gleam.py

变量 Eb/lat/lon，xarray+dask(chunks={'time':1}) 时间均值 → Flink Table + UDF。
数据缺失时清单为空即跳过。

用法:
    python gleam.py [清单名]   # 默认 gleam
"""
import os
import sys
import time

import numpy as np
import xarray as xr
from pyflink.table import EnvironmentSettings, BatchTableEnvironment, DataTypes
from pyflink.table.udf import udf

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from mgsi_common import dataio
from mgsi_common.runtime import Recorder


def load_netcdf_file(file_path):
    print(f"Loading file: {file_path}")
    try:
        with open(file_path, 'rb') as f:
            import io
            file_data = io.BytesIO(f.read())
            file_data.seek(0)

            dataset = xr.open_dataset(file_data, engine="netcdf4", chunks={'time': 1})
            lat = dataset['lat'].values
            lon = dataset['lon'].values
            precip_avg = dataset['Eb'].mean(dim='time')

            print(f"Loaded data using Dask: precip shape = {precip_avg.shape}")
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None, None, None

    return lat, lon, precip_avg


def calculate_spatial_average(precip):
    if precip is None:
        return None
    precip_mm = precip * 1000
    return np.mean(precip_mm)


@udf(input_types=[DataTypes.FLOAT(), DataTypes.FLOAT(), DataTypes.FLOAT()], result_type=DataTypes.STRING())
def query_spatial_average_udf(lat, lon, precip):
    spatial_avg = calculate_spatial_average(precip)
    return f"Spatial Average: {spatial_avg:.4f}"


def flink_job(manifest='gleam'):
    rec = Recorder('dataset_change', 'mgsi-gleam', extra={'manifest': manifest})
    total_query_time = 0
    env_settings = EnvironmentSettings.new_instance().use_blink_planner().in_batch_mode().build()
    t_env = BatchTableEnvironment.create(environment_settings=env_settings)
    t_env.get_config().get_configuration().set_string("taskmanager.memory.task.off-heap.size", "100mb")

    nc_files = dataio.list_files(manifest, suffix='.nc')
    if not nc_files:
        print(f"No .nc files found for manifest {manifest}（GLEAM 数据待下载，见 docs/DATA_ACQUISITION.md）")
        return

    for file_path in nc_files:
        lat, lon, precip_avg = load_netcdf_file(file_path)

        if lat is None or lon is None or precip_avg is None:
            continue

        start_time = time.time()

        precip_avg = precip_avg.compute()
        lat = np.array(lat)
        lon = np.array(lon)

        lat_grid, lon_grid = np.meshgrid(lat, lon, indexing="ij")
        lat_flat, lon_flat = lat_grid.ravel(), lon_grid.ravel()
        precip_flat = precip_avg.ravel()

        data = list(zip(lat_flat, lon_flat, precip_flat))

        table = t_env.from_elements(
            data,
            DataTypes.ROW([
                DataTypes.FIELD("lat", DataTypes.FLOAT()),
                DataTypes.FIELD("lon", DataTypes.FLOAT()),
                DataTypes.FIELD("Eb", DataTypes.FLOAT())
            ])
        )

        t_env.register_function("spatial_avg_query", query_spatial_average_udf)

        result = t_env.sql_query("SELECT spatial_avg_query(lat, lon, Eb) AS result FROM %s" % table)

        end_time = time.time()
        query_time = end_time - start_time
        print(f"Query time for file {os.path.basename(file_path)}: {query_time:.4f} seconds")
        total_query_time += query_time

    print(f"Total query time: {total_query_time:.4f} seconds")
    rec.mark_total('query_s', total_query_time)
    rec.finish()


if __name__ == "__main__":
    flink_job(sys.argv[1] if len(sys.argv) > 1 else 'gleam')
