"""mgsi-ncepncar 本地版 —— 复现自 D:\files\mgsi-code\dataset_change\mgsi-ncepncar.py

改动仅限数据入口（HDFS→本地清单），逻辑与计时结构保持一致。
原计时点：from_elements 前至 sql_query 构造完（不含执行）；本版额外补充修正计时
（result.to_pandas() 触发真实执行后再停表），两者都打印并记录 CSV。

用法:
    python ncepncar.py [清单名或路径]   # 默认 ncepncar
"""
import os
import sys
import time

import numpy as np
from pyflink.table import EnvironmentSettings, BatchTableEnvironment, DataTypes
from pyflink.table.udf import udf
from netCDF4 import Dataset

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from mgsi_common import dataio
from mgsi_common.runtime import Recorder


# 加载 NetCDF 文件数据（原经 HDFS 读字节流，改本地直读）
def load_netcdf_file(file_path):
    print(f"Loading file: {file_path}")
    try:
        file_data = dataio.read_bytes(file_path)
        print(f"Read {file_data.getbuffer().nbytes} bytes from {file_path}")

        if not file_data.getbuffer().nbytes:
            print(f"Error: File {file_path} is empty or could not be read.")
            return None, None, None

        file_data.seek(0)

        try:
            with Dataset('dummy', mode='r', memory=file_data.read()) as data:
                lat = data.variables['lat_2'][:]
                lon = data.variables['lon_2'][:]
                precip = data.variables['PRATE_GDS4_SFC_S113'][:, :, :]  # 加载所有时间步的数据
                print(f"First time step precip data (min, max): {np.min(precip[0])}, {np.max(precip[0])}")
        except Exception as e:
            print(f"Error reading netCDF file {file_path}: {e}")
            return None, None, None

    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None, None, None

    print(f"Loaded data: lat size = {lat.size}, lon size = {lon.size}, precip shape = {precip.shape}")
    return lat, lon, precip


# 计算空间平均值（对所有时间步的数据进行平均）
def calculate_spatial_average(precip):
    if precip is None:
        return None
    precip_mm = precip * 1000  # 将米转换为毫米
    return np.mean(precip_mm)


# 定义 Flink UDF（查询空间平均值）
@udf(input_types=[DataTypes.FLOAT(), DataTypes.FLOAT(), DataTypes.FLOAT()], result_type=DataTypes.STRING())
def query_spatial_average_udf(lat, lon, precip):
    spatial_avg = calculate_spatial_average(precip)
    return f"Spatial Average: {spatial_avg:.4f}"


# Flink 作业
def flink_job(manifest='ncepncar'):
    rec = Recorder('dataset_change', 'mgsi-ncepncar', extra={'manifest': manifest})
    total_query_time = 0
    total_query_time_fixed = 0
    # 初始化环境
    env_settings = EnvironmentSettings.new_instance().use_blink_planner().in_batch_mode().build()
    t_env = BatchTableEnvironment.create(environment_settings=env_settings)

    # 动态设置 TaskManager 的堆外内存
    t_env.get_config().get_configuration().set_string("taskmanager.memory.task.off-heap.size", "100mb")

    # 获取数据清单下所有 .nc 文件
    nc_files = dataio.list_files(manifest, suffix='.nc')

    if not nc_files:
        print(f"No .nc files found for manifest {manifest}.")
        return

    # 遍历所有 .nc 文件并处理
    for file_path in nc_files:
        lat, lon, precip = load_netcdf_file(file_path)

        start_time = time.time()  # 开始计时
        if lat is not None and lon is not None and precip is not None:
            # 转换为 Flink Table
            data = [(float(lat[i]), float(lon[j]), float(precip[:, i, j].mean()))
                     for i in range(lat.size) for j in range(lon.size)]
            table = t_env.from_elements(
                data,
                DataTypes.ROW([
                    DataTypes.FIELD("lat_2", DataTypes.FLOAT()),
                    DataTypes.FIELD("lon_2", DataTypes.FLOAT()),
                    DataTypes.FIELD("PRATE_GDS4_SFC_S113", DataTypes.FLOAT())
                ])
            )

            # 注册 UDF
            t_env.register_function("spatial_avg_query", query_spatial_average_udf)

            # 执行查询并计时（原计时：到 sql_query 构造为止）
            result = t_env.sql_query("SELECT spatial_avg_query(`lat_2`, `lon_2`, `PRATE_GDS4_SFC_S113`) AS `result` FROM `%s`" % table)
            end_time = time.time()  # 结束计时
            query_time = end_time - start_time
            print(f"Query time for file {os.path.basename(file_path)}: {query_time:.4f} seconds")
            total_query_time += query_time

            # 修正计时：触发真实执行
            fixed_start = time.time()
            pdf = result.to_pandas()
            fixed_time = time.time() - fixed_start
            total_query_time_fixed += fixed_start is not None and fixed_time or 0
            print(f"Query time (fixed, incl. execute) for file {os.path.basename(file_path)}: {fixed_time:.4f} seconds, rows={len(pdf)}")

            # 输出结果
            print(f"Results for file: {os.path.basename(file_path)}")
            pdf.head().apply(print)

    print(f"Total query time : {total_query_time:.4f} seconds")
    print(f"Total query time (fixed): {total_query_time_fixed:.4f} seconds")
    rec.mark_total('query_s', total_query_time)
    rec.mark_total('query_fixed_s', total_query_time_fixed)
    rec.finish()


# 主程序入口
if __name__ == "__main__":
    flink_job(sys.argv[1] if len(sys.argv) > 1 else 'ncepncar')
