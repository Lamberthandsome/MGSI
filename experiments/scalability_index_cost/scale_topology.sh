#!/bin/bash
# Set the TaskManager count for scalability runs: 1|2|4|8|restore
set -u
W=(65 66 71 72)
tms(){ curl -s -m 10 http://192.0.2.64:8081/overview | grep -o '"taskmanagers":[0-9]*' | cut -d: -f2; }
stop_all(){ for w in "${W[@]}"; do ssh -o BatchMode=yes -o StrictHostKeyChecking=yes root@192.0.2.$w 'for i in 1 2 3 4 5 6; do systemctl stop mgsi-flink-taskmanager@$i; done' ; done; }
start(){ ssh -o BatchMode=yes -o StrictHostKeyChecking=yes root@192.0.2.$1 "for i in $2; do systemctl start mgsi-flink-taskmanager@\$i; done"; }
wait_n(){ local want=$1 t=0; while [ "$(tms)" != "$want" ] && [ $t -lt 180 ]; do sleep 3; t=$((t+3)); done; [ "$(tms)" = "$want" ]; }
case "$1" in
  restore) stop_all; wait_n 0 || true; for w in "${W[@]}"; do start $w "1 2 3 4 5 6"; done; wait_n 24 && echo "TMS_$(tms)_OK" || echo "TMS_$(tms)_MISMATCH";;
  1) stop_all; wait_n 0 || true; start 65 "1"; wait_n 1 && echo "TMS_$(tms)_OK" || echo "TMS_$(tms)_MISMATCH";;
  2) stop_all; wait_n 0 || true; start 65 "1"; start 66 "1"; wait_n 2 && echo "TMS_$(tms)_OK" || echo "TMS_$(tms)_MISMATCH";;
  4) stop_all; wait_n 0 || true; for w in "${W[@]}"; do start $w "1"; done; wait_n 4 && echo "TMS_$(tms)_OK" || echo "TMS_$(tms)_MISMATCH";;
  8) stop_all; wait_n 0 || true; for w in "${W[@]}"; do start $w "1 2"; done; wait_n 8 && echo "TMS_$(tms)_OK" || echo "TMS_$(tms)_MISMATCH";;
  *) echo "usage: scale_topology.sh 1|2|4|8|restore"; exit 2;;
esac
