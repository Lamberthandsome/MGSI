# 缺失数据获取指南（DATA_ACQUISITION）

阶段1（纯本地）已覆盖 MERRA-2 样本 + NCEP/NCAR。以下按补全优先级排列，
每项含：下载地址、是否需账号、目标规模、落地后的清单文件。

## 1. CHIRPS GeoTIFF（补全表6，优先级最高——公开直链无需账号）
- 来源：https://www.chc.ucsb.edu/data/chirps
- 全球月均 GeoTIFF（0.05°）：
  `https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_monthly/tifs/chirps-v2.0.<YYYY>.<MM>.tif`
- 建议规模：先下 2010 年 12 个月（每月约 60-100MB，共约 1GB）
- 落地：`<dataset_dir>\CHIRPS\`，然后把每个 .tif 的绝对路径逐行写入
  `data\MANIFEST_CHIRPS.txt`，运行：
  `bash code/run.sh code/tiff/local_mgsi.py chirps`

## 2. MERRA-2 连续序列（强化表2/3，需 NASA Earthdata 账号）
- 注册：https://urs.earthdata.nasa.gov/
- wget 命令模板（1980-1984 共 1827 天，
  `--load-cookies ~/.urs_cookies` 需先在浏览器登录后导出 cookie，
  或按 GES DISC 指南配置 .netrc：https://disc.gsfc.nasa.gov/information/howto?title=How-to-Generate-Earthdata-Prerequisite-Files）
- URL 模板：`https://data.gesdisc.earthdata.nasa.gov/data/MERRA2/M2T1NXFLX.5.12.4/<YYYY>/<MM>/MERRA2_100.tavg1_2d_flx_Nx.<YYYYMMDD>.nc4`
- 建议规模：1980年1月整月逐日（31文件 × ~400MB ≈ 12GB），替换
  `data\MANIFEST_MERRA2_ONE_DAY.txt` 内容为这31个文件
- 变量：PRECTOT（无需子集，脚本按变量名读）

## 3. ECMWF ERA5（补全表2-5 的 ECMWF 列，需 Copernicus 账号）
- 注册：https://cds.climate.copernicus.eu/
- 数据：ERA5 hourly data on single levels → total precipitation（变量 `e` 是原脚本用的
  累积蒸发/降水族变量名；ERA5 命名为 tp，若坚持用 `e` 需选 "Potential evaporation"）
  原脚本读 `e(time, latitude, longitude)`（0.25°）
- 落地：`<dataset_dir>\ECMWF\*.nc`，新建 `data\MANIFEST_ECMWF.txt` 登记路径，
  并在 `code/mgsi_common/config.py` 的 MANIFESTS 加 `'ecmwf': 'data/MANIFEST_ECMWF.txt'`
- 运行：`bash code/run.sh code/dataset_change/ecmwf.py ecmwf`

## 4. GLEAM（补全 dataset_change GLEAM 行，需注册）
- 注册：https://www.gleam.eu/ （下载需发邮件申请账号）
- 数据：GLEAM v3.x daily，变量 `Eb`（terrestrial evaporation），0.25°，lat/lon 命名
- 落地：`<dataset_dir>\GLEAM\*.nc`，新建 `data\MANIFEST_GLEAM.txt` 同上登记
- 运行：`bash code/run.sh code/dataset_change/gleam.py gleam`

## 5. NCEP/NCAR 6年全量（扩展现有数据，可选）
- 本地 `<dataset_dir>\NCEPNCAR\1981.00-1986.18Z.mon.john784057.nc\`（目录名带.nc，实为文件夹）
  内有 24 个 zip（1981-1986 × 4时次），解压后登记进 `data\MANIFEST_NCEPNCAR.txt` 即可用 6 年数据
- 来源（如需更多）：https://psl.noaa.gov/data/gridded/data.ncep.reanalysis.html

## 下完后统一流程
```bash
# 更新对应清单 → 跑实验 → 重新生成报告
bash code/run_all.sh 5
bash code/run.sh code/report/build_tables.py
```
