#!/bin/bash
# MGSI 复现完整实验编排：所有实验 × 重复次数，日志独立落盘
# 用法: bash run_all.sh [重复次数，默认5]
# 结果: results/raw/*.csv（Recorder追加），results/logs/<实验>_<方法>_r<N>.log

REPS=${1:-5}
CODE_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$CODE_DIR/../results/logs"
mkdir -p "$LOG_DIR"

run() {
  local name="$1"; shift
  for ((r=1; r<=REPS; r++)); do
    echo "[$(date +%H:%M:%S)] $name rep=$r 开始"
    bash "$CODE_DIR/run.sh" "$@" > "$LOG_DIR/${name}_r${r}.log" 2>&1
    local rc=$?
    if [ $rc -ne 0 ]; then
      echo "[$(date +%H:%M:%S)] $name rep=$r 失败 (exit=$rc)，详见日志"
      echo "FAIL exit=$rc $name rep=$r" >> "$LOG_DIR/_failures.txt"
    else
      echo "[$(date +%H:%M:%S)] $name rep=$r 完成"
    fi
  done
}

# ===== 表7 消融实验（单文件，快） =====
run ablation_mgsi1          ablation/mgsi1.py merra2_single
run ablation_mgsi2          ablation/mgsi2.py merra2_single
run ablation_mgsi3          ablation/mgsi3.py merra2_single
run ablation_mgsi4          ablation/mgsi4.py merra2_single
run ablation_no_repart      ablation/no_repart.py merra2_single
run ablation_fullscan       ablation/fullscan.py merra2_single
# 消融的 MGSI(完整版) 参照行 = time_range/local_mgsi 同数据同查询
run ablation_mgsi_full      time_range/local_mgsi.py merra2_single

# ===== 表2 时间范围查询（MERRA-2 月均4文件 ≈ 降规模） =====
run time_range_mgsi         time_range/local_mgsi.py merra2_one_month
run time_range_flinksql     time_range/local_flinksql.py merra2_one_month
run time_range_spark        time_range/local_spark.py merra2_one_day

# ===== 表3 空间范围查询 R1-R8（MERRA-2 逐日，7文件降规模） =====
run spatial_mgsi            spatial_range/local_mgsi.py merra2_one_day
run spatial_flinksql        spatial_range/local_flinksql.py merra2_one_day
run spatial_spark           spatial_range/local_spark.py merra2_one_day

# ===== 表4 点查询 / 表5 k-NN =====
run point_mgsi              point/local_mgsi.py merra2_one_month
run nearest_mgsi            nearest/local_mgsi.py merra2_one_month

# ===== 数据集适配（NCEP完整 / MERRA-2样本） =====
run dschange_ncepncar       dataset_change/ncepncar.py ncepncar
run dschange_merra2         dataset_change/merra2.py merra2_one_day

echo "[$(date +%H:%M:%S)] 全部实验完成"
