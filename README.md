# MGSI

MGSI（Multi-level Grid-based Spatiotemporal Index）是一个面向流域时空数据的多级地理空间流索引系统。
它把 NetCDF / GeoTIFF 气候数据组织为逻辑分块（chunk tree），在其上维护全局质心 KD 树，
以常驻 Flink 作业的方式提供时间范围、空间范围、点查询与 k-NN 服务，并在运行时根据负载
对分块做分裂 / 合并 / 迁移（动态负载均衡）。

本仓库包含系统的完整实现、单机与集群两套实验流水线，以及各组实验的运行脚本和负载配置。

## 目录结构

```
MGSI/
├── code/                    # 单机部分（Python + Java）
│   ├── mgsi_common/         #   核心库：分块树、分块参数、全局索引、重分区策略、数据 I/O
│   ├── java/mgsi/cache/     #   托管页缓存（Flink MemoryManager 原生内存）
│   ├── time_range/          #   时间范围查询（MGSI / Flink SQL / Spark SQL）
│   ├── spatial_range/       #   空间范围查询 R1–R8
│   ├── point/  nearest/     #   点查询、k-NN（k=10）
│   ├── tiff/                #   GeoTIFF（CHIRPS）查询
│   ├── dataset_change/      #   数据集适配（ERA5、GLEAM、MERRA-2、NCEP/NCAR）
│   ├── ablation/            #   消融变体
│   ├── report/  tests/      #   结果汇总、单元测试
│   └── run.sh  run_all.sh   #   统一入口
├── server/                  # 五节点集群部分
│   ├── ResidentMGSI.java    #   常驻 MGSI Flink 作业
│   ├── SedonaMatrix.java    #   Sedona 基线
│   ├── prepare_*.py         #   HDFS 数据清点与物理索引准备
│   ├── run_*.py             #   作业提交与对照实验
│   ├── node_service.py      #   Dask / Spark 节点服务
│   └── resident_native_tenyear/  # 十年常驻变体（预初始化、合并读、空间变体）
├── experiments/             # 各组实验
│   ├── scalability_index_cost/  # 扩展性（TM 数 / 数据量）与索引构建开销
│   ├── fault_injection/         # 故障注入
│   ├── chunk_size/              # 分块大小变体
│   ├── sql_full_archive/        # 全档案 SQL 转换与计时
│   ├── ablation_gleam/          # GLEAM 消融
│   ├── ablation_merra2/         # MERRA-2 消融
│   ├── broadcast/               # 元数据广播（预分发 vs 缓存）
│   ├── dynamic_lb/              # 动态负载均衡（策略、离线验证、负载序列）
│   ├── knn_tpk/                 # k-NN（k=10）与 TPK 正式入口
│   └── knn8_tpk/                # 早期 k=8 批次（历史）
├── requirements.txt
├── LICENSE                  # Apache License 2.0
└── README.md
```

结果文件（CSV / JSONL / 日志）、编译产物（jar）和生成的物理索引不入库，均由脚本从原始数据重新生成。

## 环境要求

### 单机部分（`code/`）

- Python 3.8，Java 8
- 安装依赖：`pip install -r requirements.txt`
  （pyflink 1.13.6、pyspark 3.3.2、netCDF4、rasterio、xarray、dask 等）
- venv 必须放在纯 ASCII 路径（不含中文和空格），否则 Java classpath 会因编码问题失效

### 集群部分（`server/`、`experiments/`）

- 5 节点：1 主 4 从，每节点 24 核 / 48 GB，千兆网络
- Hadoop 2.7.2（HDFS）、Flink 1.11.3
- Flink 会话：1 个 JobManager + 24 个 TaskManager，每 TM 4 核 / 8 GB
- Sedona、Spark SQL（standalone）、Dask/Zarr 基线跑在同一集群上（拓扑见 `server/node_service.py`）
- 集群代码中的节点地址使用文档保留地址 `192.0.2.64–66/71/72`（主节点 `.64`），
  运行前替换成自己的地址；HDFS 走 WebHDFS 50070 端口，Flink REST 在 8081 端口

## 数据准备

五个数据集都是公开产品，本仓库不分发数据，按下表自行下载：

| 数据集 | 变量 | 下载地址 | 说明 |
|---|---|---|---|
| MERRA-2 | `PRECTOT`（tavg1_2d_flx_Nx） | https://disc.gsfc.nasa.gov/datasets/M2T1NXFLX.5.12.4/ | 需免费注册 NASA Earthdata |
| ERA5 | `e` | https://cds.climate.copernicus.eu/ | 需注册 Copernicus 账号 |
| CHIRPS v2.0 | 降水（日值 GeoTIFF，0.05°） | https://www.chc.ucsb.edu/data/chirps | 公开直链 |
| GLEAM | `Eb` 蒸散 | https://www.gleam.eu/ | 需邮件申请账号 |
| NCEP/NCAR R1 | `PRATE_GDS4_SFC_S113` | https://psl.noaa.gov/data/gridded/data.ncep.reanalysis.html | 公开 |

CHIRPS 全球日值 0.05° GeoTIFF 直链模板：
`https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/tifs/p05/chirps-v2.0.<YYYY>.<MM>.<DD>.tif`

MERRA-2 逐日文件 URL 模板：
`https://data.gesdisc.earthdata.nasa.gov/data/MERRA2/M2T1NXFLX.5.12.4/<YYYY>/<MM>/MERRA2_100.tavg1_2d_flx_Nx.<YYYYMMDD>.nc4`

单机部分把下载文件的绝对路径逐行写入 `data/MANIFEST_*.txt`（变量名在
`code/mgsi_common/config.py` 中登记）；集群部分上传到 HDFS 的 `/user/dataset/<名称>/`。

## 使用方法

### 单机

```bash
bash code/run.sh time_range/local_mgsi.py merra2_single   # 单个实验
bash code/run_all.sh 5                                          # 全部实验，重复 5 次
bash code/run.sh report/build_tables.py                    # 汇总生成表格
```

### 集群

1. 数据上传 HDFS 后，`server/prepare_current.py` 清点源文件生成输入清单，
   `server/prepare_matrix.py` 派生 Zarr / Sedona 表示，
   `server/prepare_physical_index.py` 准备 MGSI 物理索引；
2. `server/run_resident.py` 提交常驻 MGSI 作业，`server/run_matrix_dask.py` 与
   `SedonaMatrix.java` 运行基线，`server/controlled_pipeline.py` 串起完整对照；
3. `experiments/` 下每组实验自带 `prepare_*` / `run_*` / `analysis_*` 脚本和链式
   shell 脚本，进入对应目录按顺序执行即可复现该组实验。

单元测试：`code/tests/`（重分区策略、全局索引）；`experiments/dynamic_lb/` 自带
`fake_executor.py` 离线执行器和录制的负载序列（`workload_*.json`），不接集群即可
验证均衡逻辑。

## 实验说明

### 入口与口径

正式实验入口与口径：

- 集群对照：`server/controlled_pipeline.py`（24 TaskManager 布局）与
  `server/resident_native_tenyear/run_resident.py`（十年常驻）
- k-NN：`code/nearest/local_mgsi.py`（k=10）与 `experiments/knn_tpk/`（k=10）
- 计时：1 次预热 + 5 次正式计时，报告均值 ± 样本标准差

仓库中保留的历史版本（非正式入口，仅用于追溯）：

- `server/run_resident.py`：早期 16 TaskManager / 48 slot 布局的常驻入口
- `experiments/knn8_tpk/`：早期 k=8、单次计时 2–3 次的批次
- 部分链式脚本（如 `experiments/ablation_gleam/ablation_chain.sh`、动态负载均衡的
  对照脚本）按实验需要切换 16 TaskManager 布局，属该组实验的专用设置

- 计时协议：每个实验 1 次预热 + 5 次正式计时，报告均值 ± 样本标准差；
  每次计时前清空应用层缓存，已构建的索引 / 元数据可复用；初始化与索引构建
  不计入查询延迟，单独统计（见 `experiments/scalability_index_cost/`）。
- 托管缓存配置（`code/java/mgsi/cache/`）：8 GiB TM 进程、2 GiB 托管内存、
  512 MiB 缓存池、32 KiB 页、四槽位时每子任务 128 MiB、读取缓冲上限 16 MiB，
  详见组件内 README。

## 许可证

Apache License 2.0，见 [LICENSE](LICENSE)。

## 引用

如使用本代码，请引用：https://github.com/Lamberthandsome/MGSI （归档 DOI: [10.5281/zenodo.23429602](https://doi.org/10.5281/zenodo.23429602)）
