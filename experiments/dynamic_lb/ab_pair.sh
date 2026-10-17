#!/bin/bash
cd /opt/mgsi-experiments/dynamic_lb
P=/root/miniconda3/bin/python
META=/opt/mgsi-experiments/dynamic_lb/physical_index.with_blocks.json
JAR=/opt/mgsi-experiments/dynamic_lb/resident-mgsi.jar
rm -rf ab_off ab_on ab_pair.state
$P -u run_lb.py --meta $META --golden golden_results.json --flink-jar $JAR --workload workload_ab_off.json --output ab_off --port 38777 --expected-routes 48 --settle-seconds 30 > ab_off.log 2>&1
echo OFF_DONE >> ab_pair.state
sleep 60
$P -u run_lb.py --meta $META --golden golden_results.json --flink-jar $JAR --workload workload_ab_on.json --output ab_on --port 38778 --expected-routes 48 --settle-seconds 30 > ab_on.log 2>&1
echo ON_DONE >> ab_pair.state
