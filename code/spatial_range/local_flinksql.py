"""spatial_range/local-flinksql.py 本地版 —— 复现自 D:\files\mgsi-code\spatial_range\local-flinksql.py

Flink SQL 基线：整文件展平进 Table，R1-R8 用 SQL WHERE + AVG 查询（含 execute）。
改动仅限数据入口。

用法:
    python local_flinksql.py [清单名]   # 默认 merra2_one_day
"""
import os
import sys
import time

import numpy as np
import h5py
from pyflink.table import EnvironmentSettings, BatchTableEnvironment, DataTypes

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from mgsi_common import dataio
from mgsi_common.runtime import Recorder


def load_data(file_path):
    """加载NetCDF文件数据到内存"""
    print(f"[加载数据] 处理文件：{file_path}")
    try:
        with dataio.open_h5py(file_path) as data:
            lats = data['lat'][:]
            lons = data['lon'][:]
            precip = data['PRECTOT'][:]  # 假设维度为 (time, lat, lon)

            # 生成坐标网格
            lon_grid, lat_grid = np.meshgrid(lons, lats)
            points = np.column_stack((lon_grid.ravel(), lat_grid.ravel()))
            precip_values = precip.ravel() * 1000  # 转换为 mm 单位

            # 构建数据记录
            data_records = [
                (float(points[i][0]), float(points[i][1]), float(precip_values[i]))
                for i in range(len(points))
            ]
            return data_records
    except Exception as e:
        print(f"[错误] 读取文件失败：{e}")
        return []


def simple_flink_job(manifest='merra2_one_day'):
    rec = Recorder('spatial_range', 'flinksql', extra={'manifest': manifest})

    # 初始化Flink环境
    env_settings = EnvironmentSettings.new_instance().use_blink_planner().in_batch_mode().build()
    t_env = BatchTableEnvironment.create(environment_settings=env_settings)

    # 内存配置
    t_env.get_config().get_configuration().set_string("taskmanager.memory.process.size", "8192m")
    t_env.get_config().get_configuration().set_string("taskmanager.memory.task.off-heap.size", "1024m")

    # 定义8个空间范围的边界框 (min_lat, max_lat, min_lon, max_lon)
    BOUNDING_BOXES = [
        (-45.0, 0.0, -90.0, 90.0),  # 区域1
        (-45.0, 45.0, -90.0, 90.0),  # 区域2
        (-45.0, 90.0, -90.0, 90.0),  # 区域3
        (-90.0, 90.0, -90.0, 90.0),  # 区域4
        (-90.0, 45.0, -180.0, 120.0),  # 区域5
        (-90.0, 45.0, -180.0, 180.0),  # 区域6
        (-90.0, 90.0, -180.0, 120.0),  # 区域7
        (-90.0, 90.0, -180.0, 180.0)  # 区域8（全球）
    ]

    files = dataio.list_files(manifest, suffix='.nc4')

    # 分批次处理文件
    batch_size = 1
    times = [0, 0, 0, 0, 0, 0, 0, 0]
    for batch_idx in range(0, len(files), batch_size):
        batch_start = time.time()
        current_files = files[batch_idx:batch_idx + batch_size]
        all_records = []

        # 加载数据
        for file in current_files:
            records = load_data(file)
            if records:
                all_records.extend(records)

        # 注册Flink表
        table_start = time.time()
        table = t_env.from_elements(
            all_records,
            DataTypes.ROW([
                DataTypes.FIELD("lon", DataTypes.FLOAT()),
                DataTypes.FIELD("lat", DataTypes.FLOAT()),
                DataTypes.FIELD("precip", DataTypes.FLOAT())
            ])
        )
        table_name = f"precip_batch_{batch_idx}"
        t_env.register_table(table_name, table)

        # 对每个边界框执行查询
        for box_id, (min_lat, max_lat, min_lon, max_lon) in enumerate(BOUNDING_BOXES):
            start = time.time()

            query_sql = f"""
            SELECT
                AVG(precip) AS avg_precip
            FROM {table_name}
            WHERE
                lat BETWEEN {min_lat} AND {max_lat}
                AND lon BETWEEN {min_lon} AND {max_lon}
            """
            result = t_env.sql_query(query_sql)

            # 收集并打印结果
            result.execute()
            cur_time = time.time() - start
            times[box_id] += cur_time

    # 输出性能统计
    print(f"\n[性能报告]")
    print(times)
    for i, t in enumerate(times):
        rec.mark_total(f'R{i+1}_s', t)
    rec.finish()


if __name__ == "__main__":
    total_start = time.time()
    simple_flink_job(sys.argv[1] if len(sys.argv) > 1 else 'merra2_one_day')
    print(f"总运行时间: {time.time() - total_start:.2f}s")
    dataio.close_all()
