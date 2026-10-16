#!/bin/bash
cd /opt/mgsi-experiments/20261014-dynamic-lb
P=/root/miniconda3/bin/python
META=/opt/mgsi-experiments/20261014-dynamic-lb/physical_index.with_blocks.json
JAR=/opt/mgsi-experiments/20261014-dynamic-lb/resident-mgsi.jar
rm -rf ab32_off ab32_on ab32_pair.state
$P -u run_lb.py --meta $META --golden golden_results.json --flink-jar $JAR --workload workload_ab32_off.json --output ab32_off --port 38779 --expected-routes 48 --settle-seconds 30 > ab32_off.log 2>&1
echo OFF_DONE >> ab32_pair.state
sleep 60
$P -u run_lb.py --meta $META --golden golden_results.json --flink-jar $JAR --workload workload_ab32_on.json --output ab32_on --port 38780 --expected-routes 48 --settle-seconds 30 > ab32_on.log 2>&1
echo ON_DONE >> ab32_pair.state
