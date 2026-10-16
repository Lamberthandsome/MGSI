"""time_range/local-flinksql.py 本地版 —— 复现自 D:\files\mgsi-code\time_range\local-flinksql.py

Flink SQL 基线：整文件逐点展平进 Table + SQL WHERE 过滤。
改动仅限数据入口，分批 batch_size、计时结构与原脚本一致（原计时含 execute）。

用法:
    python local_flinksql.py [清单名]   # 默认 merra2_one_month
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


def load_data(file_path):
    """直接加载NetCDF文件到内存"""
    print(f"[加载数据] 开始处理文件：{file_path}")
    try:
        with dataio.open_h5py(file_path) as data:
            # 读取原始数据
            lats = data['lat'][:]
            lons = data['lon'][:]
            precip = data['PRECTOT'][:]  # 假设维度为 (time, lat, lon)

            # 生成坐标网格
            lon_grid, lat_grid = np.meshgrid(lons, lats)
            points = np.column_stack((lon_grid.ravel(), lat_grid.ravel()))
            precip_values = precip.ravel() * 1000  # 转换为 mm 单位

            # 构建数据表
            data_records = [
                (float(points[i][0]), float(points[i][1]), float(precip_values[i]))
                for i in range(len(points))
            ]
            return data_records
    except Exception as e:
        print(f"[错误] 读取文件 {file_path} 失败：{e}")
        return []


@udf(input_types=[DataTypes.FLOAT(), DataTypes.FLOAT(), DataTypes.FLOAT()],
     result_type=DataTypes.STRING())
def enhanced_query_udf(lon, lat, precip):
    """增强的查询UDF（保持原始逻辑）"""
    return f"Enhanced Value: {precip:.4f} mm"


def simple_flink_job(manifest='merra2_one_month'):
    rec = Recorder('time_range', 'flinksql', extra={'manifest': manifest})
    load_time = 0
    register_time = 0
    query_time = 0

    # 初始化单节点环境
    env_settings = EnvironmentSettings.new_instance().use_blink_planner().in_batch_mode().build()
    t_env = BatchTableEnvironment.create(environment_settings=env_settings)

    # 增加内存配置
    t_env.get_config().get_configuration().set_string("taskmanager.memory.process.size", "8192m")
    t_env.get_config().get_configuration().set_string("taskmanager.memory.task.off-heap.size", "1024m")

    files = dataio.list_files(manifest, suffix='.nc4')

    # 分批次加载数据
    batch_size = 2
    for i in range(0, len(files), batch_size):
        start_time = time.time()
        batch_files = files[i:i + batch_size]
        all_data_records = []
        for f in batch_files:
            data = load_data(f)
            if data:
                all_data_records.extend(data)

        load_time += time.time() - start_time
        start_time = time.time()

        # 创建统一数据表
        table = t_env.from_elements(
            all_data_records,
            DataTypes.ROW([
                DataTypes.FIELD("lon", DataTypes.FLOAT()),
                DataTypes.FIELD("lat", DataTypes.FLOAT()),
                DataTypes.FIELD("precip", DataTypes.FLOAT())
            ])
        )
        t_env.register_table(f"merged_precip_table_{i}", table)

        # 注册UDF
        t_env.register_function("enhanced_query", enhanced_query_udf)

        # 科罗拉多区域查询条件
        query_sql = f"""
        SELECT
            enhanced_query(`lon`, `lat`, `precip`) AS `result`
        FROM merged_precip_table_{i}
        WHERE
            `lon` BETWEEN -109.0 AND -102.0
            AND `lat` BETWEEN 38.0 AND 41.0
        """

        register_time += time.time() - start_time
        start_time = time.time()

        # 执行查询
        print(f"[主流程] 开始执行批次 {i // batch_size + 1} 的查询...")
        result = t_env.sql_query(query_sql)
        result.execute()

        query_time += time.time() - start_time

    rec.mark_total('load_s', load_time)
    rec.mark_total('register_s', register_time)
    rec.mark_total('query_s', query_time)
    print(f"[耗时计算] 并行读取和处理文件+构建本地索引：{load_time * 1000:.2f}ms")
    print(f"[耗时计算] 创建FlinkTable&注册UDF：{register_time * 1000:.2f}ms")
    print(f"[耗时计算] 内存查询：{query_time * 1000:.2f}ms")
    rec.finish()


if __name__ == "__main__":
    manifest = sys.argv[1] if len(sys.argv) > 1 else 'merra2_one_month'
    start_time = time.time()
    simple_flink_job(manifest)
    print(f"[耗时计算] 端到端总耗时：{(time.time() - start_time) * 1000:.2f}ms")
    dataio.close_all()
