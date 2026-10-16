# MGSI 论文本地复现报告

> 2026-10-06 23:40：GLEAM 1981–1990 原始全年文件的常驻 Java MGSI 已完成八区域各六次、共 48 次逐月数值核验。首次 R1/R8 为 5.373/14.710 秒，初始化和物理索引准备另记；最终入口在 `20261006-resident-native/preinitialized/`。四版原生及 SQL 共 182 次、21,840 条月结果通过。SQL 只保存 120 个查询日期，完整归档点表与公平计时仍待统一；没有恢复旧数据版本/完整动态迁移机制。最新结果与限制见 [十年复现修复记录](../server_experiments/resident_native_20261006/十年复现修复记录.md)。下方单机范围表保留为早期历史。

> 2026-10-06 更新：下文的单机运行记录属于早期移植阶段。五节点服务器的最新代码、计时及五数据集核查见 [跨基线与数据集核查](../server_experiments/cross_baseline_audit_20261006/跨基线与数据集核查.md)。保留旧脚本的计时点不能保证与大论文可比；旧脚本部分记录尚未触发查询或未等到结果返回。正式对照以正确查询语义和完整结果返回为准。

复现对象：*MGSI: Multi-level Geospatial Stream Index with Dynamic Load Balancing for Watershed Spatiotemporal Data*
（本仓库 `code/` 目录）

## 1. 复现范围

| 论文表 | 实验 | 复现状态 | 数据 |
|---|---|---|---|
| 表2 | 时间范围查询（5方法×4数据集×2档） | 部分：MGSI/FlinkSQL/Spark × MERRA-2 | MERRA-2月均2010.01-04（降规模） |
| 表3 | 空间范围R1-R8 | 部分：3方法 × MERRA-2 | MERRA-2逐日7文件 |
| 表4 | 点查询 | 部分：MGSI × MERRA-2 | 同表2 |
| 表5 | k-NN(k=8) | 部分：MGSI × MERRA-2 | 同表2 |
| 表6 | GeoTIFF查询 | **未跑**（代码就绪） | CHIRPS缺失 |
| 表7 | 消融（6变体） | **完整**：4原变体+全扫+新增Remove Dynamic Repartitioning | 单文件19800115 |

缺失部分的原因与补全路径见 `DEVIATIONS.md` 与 `DATA_ACQUISITION.md`。
未复现的基线：SciSpark（独立项目）、Hierarchy Index（本机无代码）。

## 2. 环境与运行方式

- Windows 11 单机，Python 3.8.0（venv 置于 ASCII 路径），Java 8，pyflink 1.13.6，pyspark 3.3.2
- 所有脚本统一入口：
  ```bash
  bash code/run.sh code/<实验目录>/<脚本>.py [清单名]
  ```
- 全量实验（约数小时）：
  ```bash
  bash code/run_all.sh 5        # 5次重复
  bash code/run.sh code/report/build_tables.py
  ```
- 数据文件用清单登记（`data/MANIFEST_*.txt`），不复制大文件

## 3. 移植原则

- 早期移植保留实验 main 流程、分块参数、查询参数与计时点，用于核查原脚本行为；其可比性需另外验证，不能由逐行一致推定。
- 仅替换数据入口：`hdfs.InsecureClient.read → 本地 open`；Spark `binaryFiles(hdfs://) → parallelize(本地路径)`
- 早期移植只修运行错误，原始行为单独留档；服务器正式对照需修正漏读时间步、坐标错配、缺失值和计时缺陷（见最新核查）。
- 每次运行的分段耗时自动追加到 `results/raw/<实验>.csv`，同时保留原计时口径与修正计时双列

## 4. 结果

（实验运行中，本节由 `results/tables/` 汇总生成——见 summary_trends.md 与各 table*_repro 文件）

### 4.1 消融实验（表7对应）
### 4.2 时间范围查询（表2对应）
### 4.3 空间范围查询（表3对应）
### 4.4 点查询与k-NN（表4/5对应）

## 5. 结论

（待结果汇总后填写：趋势是否与论文一致）
