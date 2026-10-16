#!/bin/bash
cd /opt/mgsi-experiments/20261014-dynamic-lb
P=/root/miniconda3/bin/python
META=/opt/mgsi-experiments/20261014-dynamic-lb/physical_index.with_blocks.json
JAR=/opt/mgsi-experiments/20261014-dynamic-lb/resident-mgsi.jar
run() { rm -rf $1; $P -u run_lb.py --meta $META --golden golden_results.json --flink-jar $JAR --workload $2 --output $1 --port $3 --expected-routes 48 --settle-seconds 30 > $1.log 2>&1; echo "$1 rc=$? $(date +%H:%M)" >> overnight.state; }
rm -f overnight.state
run steady_off workload_steady_off.json 38781
sleep 60
run steady_on workload_steady_on.json 38782
sleep 60
run steady_shift workload_steady_shift.json 38783
sleep 60
run p64_off workload_p64_off.json 38784
sleep 60
run p64_on workload_p64_on.json 38785
echo ALL_DONE >> overnight.state
