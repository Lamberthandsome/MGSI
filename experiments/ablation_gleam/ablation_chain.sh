#!/bin/bash
# Ablation chain: 16-TM layout -> GLEAM run -> MERRA-2 run -> restore 24 TM.
set -u
R=/opt/mgsi-experiments
P=/root/miniconda3/bin/python
W="65 66 71 72"
STATE=$R/ablation_chain.state
log(){ echo "$(date '+%F %T') $1" >> $STATE; }
tms(){ curl -s -m 10 http://192.0.2.64:8081/overview | grep -o '"taskmanagers":[0-9]*' | cut -d: -f2; }
slots(){ curl -s -m 10 http://192.0.2.64:8081/overview | grep -o '"slots-available":[0-9]*' | cut -d: -f2; }
wait_for(){ # wait_for <tms> <timeout_s> <label>
  local want=$1 t=$2 label=$3 i=0
  while [ "$(tms)" != "$want" ] && [ $i -lt $t ]; do sleep 3; i=$((i+3)); done
  [ "$(tms)" = "$want" ] && return 0 || { log "WAIT_FAIL_$label_TMS_$(tms)"; return 1; }
}
restore(){
  for w in $W; do ssh -o BatchMode=yes -o StrictHostKeyChecking=yes root@192.0.2.$w 'systemctl start mgsi-flink-taskmanager@5 mgsi-flink-taskmanager@6' ; done
  wait_for 24 120 RESTORE || true
  log "RESTORED_TMS_$(tms)_SLOTS_$(slots)"
}
log "CHAIN_START"
# Stopping @5/@6 cascades through the group unit (Requires=): all TMs go down.
for w in $W; do ssh -o BatchMode=yes -o StrictHostKeyChecking=yes root@192.0.2.$w 'systemctl stop mgsi-flink-taskmanager@5 mgsi-flink-taskmanager@6' ; done
wait_for 0 120 STOP || { log "ABORT_STOP"; restore; exit 1; }
log "ALL_TM_DOWN"
for w in $W; do ssh -o BatchMode=yes -o StrictHostKeyChecking=yes root@192.0.2.$w 'systemctl start mgsi-flink-taskmanager@1 mgsi-flink-taskmanager@2 mgsi-flink-taskmanager@3 mgsi-flink-taskmanager@4' ; done
wait_for 16 180 START16 || { log "ABORT_START16"; restore; exit 1; }
if [ "$(slots)" != "48" ]; then log "ABORT_SLOTS_$(slots)"; restore; exit 1; fi
log "LAYOUT_OK_16TM_48SLOTS"
cd $R/ablation_gleam && $P -u run_ablation.py gleam > ablation_run.log 2>&1
log "GLEAM_RC_$?"
cd $R/ablation_merra2 && $P -u run_ablation.py merra2 > ablation_run.log 2>&1
log "MERRA2_RC_$?"
restore
log "CHAIN_DONE"
