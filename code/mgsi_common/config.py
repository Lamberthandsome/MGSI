"""MGSI 复现 —— 全局配置。

所有硬编码参数收敛于此，默认值与系统设计一致。
"""

# ============ 查询参数（原代码硬编码值） ============
# 科罗拉多区域 bbox（原 local-mgsi.py 示例查询）
COLORADO_BBOX = {
    'lon_min': -109.0, 'lon_max': -102.0,
    'lat_min': 38.0, 'lat_max': 41.0,
}
# 点查询目标（原 point/local-mgsi.py、nearest/local-mgsi.py）
QUERY_POINT = (-174.5, 80.5)  # (lon, lat)
# k-NN 的 k
KNN_K = 10
# 逻辑分块数（每维）
CHUNK_SIZE = 16
# 全局索引查询半径缓冲（度）
QUERY_BUFFER = 5.0
# 数值单位转换（m -> mm）
UNIT_SCALE = 1000

# 空间范围查询 R1-R8（原 spatial_range/local-mgsi.py 中的 bboxes：
# ((lat_min, lon_min), (lat_max, lon_max)) 展开）
SPATIAL_RANGES = [
    {'lat_min': -45, 'lon_min': -90,  'lat_max': 0,  'lon_max': 90},   # R1
    {'lat_min': -45, 'lon_min': -90,  'lat_max': 45, 'lon_max': 90},   # R2
    {'lat_min': -45, 'lon_min': -90,  'lat_max': 90, 'lon_max': 90},   # R3
    {'lat_min': -90, 'lon_min': -90,  'lat_max': 90, 'lon_max': 90},   # R4
    {'lat_min': -90, 'lon_min': -180, 'lat_max': 45, 'lon_max': 120},  # R5
    {'lat_min': -90, 'lon_min': -180, 'lat_max': 45, 'lon_max': 180},  # R6
    {'lat_min': -90, 'lon_min': -180, 'lat_max': 90, 'lon_max': 120},  # R7
    {'lat_min': -90, 'lon_min': -180, 'lat_max': 90, 'lon_max': 180},  # R8
]

# ============ 数据集配置 ============
# 数据清单文件（data/ 目录），替代原 HDFS 目录列表
MANIFESTS = {
    # 原 /user/dataset/merra2_one_day/ —— MERRA-2 逐日样本
    'merra2_one_day': 'data/MANIFEST_MERRA2_ONE_DAY.txt',
    # 原 /user/dataset/merra2_one_month/ —— MERRA-2 月均样本
    'merra2_one_month': 'data/MANIFEST_MERRA2_ONE_MONTH.txt',
    # 原 /user/dataset/ncepncar/ —— NCEP/NCAR 数据
    'ncepncar': 'data/MANIFEST_NCEPNCAR.txt',
    # 单文件（消融冒烟/调试用，19800115）
    'merra2_single': 'data/MANIFEST_MERRA2_SINGLE.txt',
    # 原 /user/dataset/chirps/ —— CHIRPS GeoTIFF（数据待下载，清单为空）
    'chirps': 'data/MANIFEST_CHIRPS.txt',
}

# 各数据集的变量名（原脚本中的读取变量）
VARIABLES = {
    'merra2': 'PRECTOT',
    'ncepncar': 'PRATE_GDS4_SFC_S113',
    'ecmwf': 'e',
    'gleam': 'Eb',
}

# ============ 环境 ============
# 可选修复 N+1 读放大：默认关闭以保持与原代码行为一致
import os as _os
FIX_READ_AMP = _os.environ.get('MGSI_FIX_READ_AMP', '0') == '1'
