"""消融对照：无索引全表扫描 —— 移植自 D:\files\mgsi-code\ablation\mgsi-2-1.py（同族2-2/2-3/2-4与其字节级相同或仅注释差异）

不做任何分块/索引：整文件逐点展平，bbox掩膜过滤后进Flink Table跑UDF。
原脚本跑R1-R8；本移植在消融框架下只跑科罗拉多bbox（与其他消融变体同口径）。
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
from _ablation_common import QUERY_BBOX


def load_netcdf_file(file_path):
    try:
        with dataio.open_h5py(file_path) as data:
            lat = data['lat'][:]
            lon = data['lon'][:]
            humidity = data['PRECTOT'][:, :, :]
            return lat, lon, humidity
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None, None, None


def filter_region(lat, lon, humidity, lat_min, lat_max, lon_min, lon_max):
    if lat is None or lon is None or humidity is None:
        return None, None, None

    lon_grid, lat_grid = np.meshgrid(lon, lat)
    mask = (lat_grid >= lat_min) & (lat_grid <= lat_max) & (lon_grid >= lon_min) & (lon_grid <= lon_max)

    filtered_lat = lat_grid[mask]
    filtered_lon = lon_grid[mask]
    filtered_humidity = humidity[:, mask]

    return filtered_lat, filtered_lon, filtered_humidity


def calculate_spatial_average(humidity):
    return np.mean(humidity) if humidity is not None else None


@udf(input_types=[DataTypes.FLOAT(), DataTypes.FLOAT(), DataTypes.FLOAT()], result_type=DataTypes.STRING())
def query_spatial_average_udf(lat, lon, humidity):
    spatial_avg = calculate_spatial_average(humidity)
    return f"Spatial Average: {spatial_avg:.4f}" if spatial_avg is not None else "No data"


def flink_job(manifest='merra2_one_day'):
    rec = Recorder('ablation', 'no_index_fullscan', extra={'manifest': manifest})
    total_time = 0
    env_settings = EnvironmentSettings.new_instance().use_blink_planner().in_batch_mode().build()
    t_env = BatchTableEnvironment.create(environment_settings=env_settings)
    t_env.get_config().get_configuration().set_string("taskmanager.memory.process.size", "256m")
    t_env.get_config().get_configuration().set_string("taskmanager.memory.task.off-heap.size", "128m")

    files = dataio.list_files(manifest, suffix='.nc4')

    for file_path in files:
        lat, lon, humidity = load_netcdf_file(file_path)
        if lat is None:
            continue

        start = time.time()
        b = QUERY_BBOX
        lat_f, lon_f, humidity_f = filter_region(lat, lon, humidity, b['lat_min'], b['lat_max'], b['lon_min'], b['lon_max'])

        if lat_f is None:
            continue

        data = [
            (float(lat_f[i]), float(lon_f[i]), float(humidity_f[:, i].mean()))
            for i in range(len(lat_f))
        ]

        table = t_env.from_elements(
            data,
            DataTypes.ROW([
                DataTypes.FIELD("lat", DataTypes.FLOAT()),
                DataTypes.FIELD("lon", DataTypes.FLOAT()),
                DataTypes.FIELD("humidity", DataTypes.FLOAT())
            ])
        )

        t_env.register_function("spatial_avg_query", query_spatial_average_udf)
        result = t_env.sql_query(
            f"SELECT spatial_avg_query(`lat`, `lon`, `humidity`) AS `result` FROM `{table}`"
        )
        total_time += time.time() - start

    total_ms = total_time * 1000
    print(f"总执行时间：{total_ms:.1f}ms")
    rec.mark_total('total_ms', total_ms)
    rec.finish()


if __name__ == "__main__":
    flink_job(sys.argv[1] if len(sys.argv) > 1 else 'merra2_one_day')
    dataio.close_all()
