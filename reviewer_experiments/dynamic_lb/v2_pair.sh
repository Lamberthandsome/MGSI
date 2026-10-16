#!/bin/bash
cd /opt/mgsi-experiments/20261014-dynamic-lb
P=/root/miniconda3/bin/python
META=/opt/mgsi-experiments/20261014-dynamic-lb/physical_index.with_blocks.json
JAR=/opt/mgsi-experiments/20261014-dynamic-lb/resident-mgsi.jar
rm -f v2_pair.state
run() { rm -rf $1; $P -u run_lb.py --meta $META --golden golden_results.json --flink-jar $JAR --workload $2 --output $1 --port $3 --expected-routes 48 --settle-seconds 30 > $1.log 2>&1; echo "$1 rc=$? $(date +%H:%M)" >> v2_pair.state; }
run steady_off_v2 workload_steady_off_v2.json 38786
sleep 60
run steady_on_v2 workload_steady_on_v2.json 38787
echo V2_DONE >> v2_pair.state
