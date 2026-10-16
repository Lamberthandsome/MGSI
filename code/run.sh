#!/bin/bash
# MGSI 复现实验统一运行入口
# 用法: bash run.sh <脚本相对code/的路径> [参数...]
#   例: bash run.sh dataset_change/ncepncar.py
#       bash run.sh time_range/local_mgsi.py merra2_one_month
#
# 环境要点（已验证）:
#  1. venv Scripts 放 PATH 最前 —— pyflink 起的 UDF worker 需要 "python" 解析到 venv
#  2. _JAVA_OPTIONS=-Dfile.encoding=UTF-8 —— 项目路径含中文（"项目"），
#     py4j 传给 JVM 的路径字符串按 GBK 解码会乱码导致 CreateProcess error=2
#  3. PYTHONIOENCODING=utf-8 —— 控制台中文输出
#  4. no_proxy=* —— 本机注册表残留 clash 代理(127.0.0.1:7897)会让 pip 失效（仅装包时需要）

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# venv 放在 ASCII 路径 D:\mgvenv（Python会解析回真实路径，项目路径含中文
# 会导致 Java classpath/进程路径乱码，Spark 与 pyflink worker 均会失败）
VENV_DIR="D:/mgvenv"
export PATH="$VENV_DIR/Scripts:$PATH"
export _JAVA_OPTIONS="-Dfile.encoding=UTF-8"
export PYTHONIOENCODING=utf-8
export PYSPARK_PYTHON="D:/mgvenv/Scripts/python.exe"
export PYSPARK_DRIVER_PYTHON="D:/mgvenv/Scripts/python.exe"
# winutils（pyspark 在 Windows 本地模式必需）
export HADOOP_HOME="$(cd "$SCRIPT_DIR/.." && pwd)/hadoop"
export PATH="$HADOOP_HOME/bin:$PATH"

cd "$SCRIPT_DIR"
exec "$VENV_DIR/Scripts/python.exe" "$@"
