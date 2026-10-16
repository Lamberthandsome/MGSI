# 复现偏离说明（DEVIATIONS）

> 2026-10-06 23:40：五节点 GLEAM 十年常驻 Java 物理索引版本已通过 48 次完整查询，R4–R8 首次时间已接近或低于论文记录，R1–R3 仍偏慢。旧 GLEAM 数据版本、历史缓存/初始化边界仍不明；当前堆缓存和固定所有权不等同于论文的托管内存、动态迁移和 checkpoint 恢复。SQL 新查询数值通过，但 Parquet 只含 120 个查询日，未构建全年所有日期点表，且 Flink 新作业/Spark 常驻应用边界不同。详见 [十年复现修复记录](../server_experiments/resident_native_20261006/十年复现修复记录.md)。本节更新优先于下文早期单机假设。

> 2026-10-06 更新：本文件主要记录早期单机移植。五节点服务器的最新核查见 [跨基线与数据集核查](../server_experiments/cross_baseline_audit_20261006/跨基线与数据集核查.md)。旧行为保留用于诊断，含漏读、错配和未完成查询的耗时不能作为正确 baseline 或正式论文对照。

本复现为**本地单机版**：原实验在 1 JobManager + 4 TaskManager 的 Flink 1.11.3 / Hadoop 2.7.2 集群上运行，
单机绝对耗时与论文数字不可直接比较；验收标准为**方法间相对趋势一致**（用户确认）。
以下按类别列出所有与原代码/论文设定的偏离。

## 1. 环境偏离
| 项 | 原实验 | 本复现 | 影响 |
|---|---|---|---|
| 计算 | 5节点集群, 24核×48GB/节点 | 单机 Windows, 16GB内存 | 绝对耗时完全不同；无跨节点网络I/O |
| Flink | 1.11.3 集群 standalone | pyflink 1.13.6 本地批（MiniCluster） | API兼容（blink planner/BatchTableEnvironment 均在） |
| Spark | 集群 + binaryFiles(hdfs://) | pyspark 3.3.2 local[1] + parallelize(本地路径) | 数据读入方式等价（本地磁盘代替HDFS） |
| HDFS | WebHDFS 50070 + RPC 9000 | 本地文件系统（清单文件登记散落数据） | 读取链 `InsecureClient→BytesIO→h5py` 改为 `open→BytesIO→h5py`，语义等价 |
| SciSpark / Hierarchy Index | 论文中的另两个基线 | **未复现**：原电脑上无对应可运行代码（SciSpark 是独立项目，Hierarchy Index 的 Spark 实现在本机未找到） | 表2/3/4/5/6 中这两行缺失 |

## 2. 数据偏离（规模缩减）
| 实验 | 原设定 | 本复现 | 说明 |
|---|---|---|---|
| 表2 时间范围 MERRA-2 | 1年/10年 hourly（1.33TB） | 4个月均文件（2010.01-04, ~220MB） | "1 Year"档≈4个月月均 |
| 表3 空间范围 MERRA-2 | 全量逐日 | 7个零星逐日文件（1980.01的7天, ~2.8GB） | R1-R8结构完整 |
| 表4/5 点/kNN | 各数据集 | MERRA-2月均4文件 | ECMWF列缺失（无数据） |
| 表6 GeoTIFF (CHIRPS) | 10年 daily | **未运行**（CHIRPS本地为空） | 代码已移植（code/tiff/），数据到位即可跑 |
| dataset_change | 5数据集 | NCEP/NCAR（1981年4季度nc）+ MERRA-2样本 | ECMWF/GLEAM 代码移植完成但无数据 |
| 消融（表7） | MERRA-2 10年查询 | 单文件（19800115.nc4） | 变体间对比不受影响 |

## 3. 代码修正（对原代码的改动，均经确认）
1. **mgsi2.py 语法错误修复**：原文件末尾粘贴了裸数字（`4107`、`14626.3ms`...，疑为粘贴的实验结果），导致
   SyntaxError 无法运行。已删除。这些数字本身记录在本文档此处留档。
2. **no_repart.py 为新增**：论文表7的 Remove Dynamic Repartitioning 变体在原代码中**不存在**（原 ablation
   目录只有4个变体+全扫对照）。按论文 4.4 节描述实现：保留三级索引，数据加载不做动态按需调度，
   改为静态全量建表 + SQL WHERE 过滤（对应"static range-based strategy"）。
3. **mgsi-2-2/2-3/2-4 未单独移植**：与 mgsi-2-1 字节级相同或仅注释差异，统一为 fullscan.py。
4. **point/local-mgsi-ecmwf.py 未移植**：原文件调用了未定义的方法（AttributeError），且 ECMWF 数据缺失。
   如后续拿到数据可从 point/local_mgsi.py 改变量名得到。
5. **计时偏差保留并补测**：原脚本部分计时点在惰性执行（`result.execute()` / `to_pandas()`）之前，
   SQL 实际执行未计入"总查询时间"（如 mgsi-ncepncar 的 `to_pandas()` 被注释掉）。
   早期输出保留原计时列供诊断；服务器对照以实际执行并完整返回结果后停表为准，不能将旧口径视为与论文可比的主结果。
   两者差距见 results/raw 中 query_s 与 query_fixed_s。

## 4. 已知行为保留（原代码即如此）
- **mgsi1（Remove Logical Chunking）对全球文件的区域查询可能返回空**：其代表点取文件中心（≈(0,0)），
  科罗拉多查询半径（max边/2×1.5=5.25°）内无代表点 → 无命中。这是原代码逻辑的真实行为，
  也侧面说明逻辑分块的必要性（消融正是要展示这一点）。
- **flinksql 基线的点展平只用到第一个时间步**：`precip.ravel()`（含时间维）与网格点数（不含时间维）
  错位，`range(len(points))` 只取了第一个时间步的数据。保留原行为。
- **N+1 读放大**：`load_data_to_memory` 对每个命中分块重复整读同一文件。保留原行为；
  可设环境变量 `MGSI_FIX_READ_AMP=1` 开启文件句柄缓存（默认关闭，避免影响可比性）。

## 5. 平台适配（Windows 特有）
- venv 必须在 ASCII 路径（如 D:\mgvenv 等 ASCII 路径）：Python 将解析回真实路径，中文路径会使 Java classpath 乱码。
- `_JAVA_OPTIONS=-Dfile.encoding=UTF-8`：py4j 传给 JVM 的路径字符串按 GBK 解码会乱码。
- `HADOOP_HOME` 指向项目 hadoop/ 目录（winutils.exe + hadoop.dll，pyspark Windows 必需）。
- pip 安装需 `no_proxy=*`：系统注册表残留 clash 代理（127.0.0.1:7897），urllib 走它下载会失败，
  需用镜像源（清华）并屏蔽代理。
- 以上已固化在 code/run.sh。
