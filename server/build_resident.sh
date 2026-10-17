#!/usr/bin/env bash
set -euo pipefail

task_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
task_flink="${FLINK_HOME:-/opt/mgsi-cluster/flink}"
task_hadoop="${HADOOP_HOME:-/app/hadoop-2.7.2}"
task_build="${task_dir}/.resident_build"
mkdir -p -- "${task_build}"

if [[ ! -d "${task_flink}/lib" ]]; then
    echo "Set FLINK_HOME to the Flink 1.11.3 installation." >&2
    exit 1
fi
if [[ -n "${HADOOP_CLASSPATH:-}" ]]; then
    task_hadoop_cp="${HADOOP_CLASSPATH}"
elif [[ -x "${task_hadoop}/bin/hadoop" ]]; then
    task_hadoop_cp="$("${task_hadoop}/bin/hadoop" classpath --glob)"
else
    echo "Set HADOOP_CLASSPATH or HADOOP_HOME for Hadoop 2.7.2." >&2
    exit 1
fi

task_javac="${JAVA_HOME:+${JAVA_HOME}/bin/}javac"
task_jar="${JAVA_HOME:+${JAVA_HOME}/bin/}jar"
"${task_javac}" -encoding UTF-8 -source 8 -target 8 \
    -cp "${task_flink}/lib/*:${task_hadoop_cp}" -d "${task_build}" \
    "${task_dir}/JobManagerGlobalIndexSource.java" "${task_dir}/ResidentMGSI.java"
"${task_jar}" cf "${task_dir}/resident-mgsi.jar" -C "${task_build}" .
