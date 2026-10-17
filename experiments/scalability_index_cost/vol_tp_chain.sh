#!/bin/bash
# P57(b) volume-axis throughput: vol1yr_tp + vol5yr_tp at 8 one-slot TMs.
# Waits for the ablation chain to finish, then runs,
# then restores the 24 TM / 72 slot (3-slot) topology via topology.sh
# plus unmask/start of tm-6 on 71/72.
set -u
R=/opt/mgsi-experiments/scalability
P=/root/miniconda3/bin/python
STATE=/opt/mgsi-experiments/vol_tp.state
LOG=$R/vol_tp_chain.log
note(){ echo "$(date '+%F %T') $1" >> $STATE; }
cd $R
note "WATCHER_START"

# --- Phase 0: wait for the ablation chain ---
deadline=$(date -d '2026-10-17 12:00' +%s)
while :; do
  done_flag=no; pgrep -f v3_chain_quick.sh >/dev/null 2>&1 || done_flag=yes
  running=$(curl -s -m 10 http://192.0.2.64:8081/overview | grep -o '"jobs-running":[0-9]*' | cut -d: -f2)
  if grep -qE 'CHAIN_DONE|CHAIN_ABORTED' /opt/mgsi-experiments/ablation_v3_chain.state 2>/dev/null && [ "$done_flag" = yes ] && [ "${running:-1}" = 0 ]; then break; fi
  if [ $(date +%s) -ge $deadline ]; then note "WATCHER_TIMEOUT_WAITING_V3"; exit 1; fi
  sleep 60
done
note "V3_CHAIN_CONFIRMED_DONE"
sleep 30
df_pct=$(df -P / | awk 'NR==2{gsub("%","",$5); print $5}')
if [ "${df_pct:-0}" -ge 90 ]; then note "ABORT_DISK_${df_pct}PCT"; exit 1; fi
running=$(curl -s -m 10 http://192.0.2.64:8081/overview | grep -o '"jobs-running":[0-9]*' | cut -d: -f2)
[ "${running:-1}" = 0 ] || { note ABORT_JOB_STILL_RUNNING; exit 1; }

# --- Phase 1: tm-1/tm-2 -> 1 slot (backup first) ---
for w in 65 66 71 72; do
  ssh -o BatchMode=yes -o StrictHostKeyChecking=yes root@192.0.2.$w '
    for i in 1 2; do
      c=/opt/mgsi-cluster/flink/taskmanagers/tm-$i/conf/flink-conf.yaml
      [ -f $c.preslot_tp.bak ] || cp $c $c.preslot_tp.bak
      sed -i "s/^taskmanager.numberOfTaskSlots:.*/taskmanager.numberOfTaskSlots: 1/" $c
    done'
done
note "ONESLOT_CONF_SET"

# --- Phase 2: topology 8 one-slot TMs ---
bash scale_topology.sh 8 >> $LOG 2>&1
tms=$(curl -s -m 10 http://192.0.2.64:8081/overview | grep -o '"taskmanagers":[0-9]*' | cut -d: -f2)
slots=$(curl -s -m 10 http://192.0.2.64:8081/overview | grep -o '"slots-available":[0-9]*' | cut -d: -f2)
note "TOPO8_TMS_${tms}_SLOTS_${slots}"
[ "$tms" = 8 ] && [ "$slots" = 8 ] || { note ABORT_TOPO_MISMATCH; exit 1; }

# --- Phase 3: the two throughput runs ---
for K in 1 5; do
  port=$((38817 + K))
  $P -u run_scalability.py --tms 8 --label vol${K}yr_tp --meta physical_index_vol${K}yr.json       --golden-extra golden_extra_vol${K}yr.json --throughput-seconds 120 --port $port > run_vol${K}yr_tp.log 2>&1
  note "VOL${K}YR_TP_RC_$?"
done

# --- Phase 4: restore 24 TM / 72 slots ---
bash /opt/mgsi-experiments/ablation_followup/topology.sh restore >> $LOG 2>&1
for w in 71 72; do
  ssh -o BatchMode=yes -o StrictHostKeyChecking=yes root@192.0.2.$w 'systemctl unmask mgsi-flink-taskmanager@6; systemctl start mgsi-flink-taskmanager@6' >> $LOG 2>&1
done
t=0
while [ $t -lt 180 ]; do
  tms=$(curl -s -m 10 http://192.0.2.64:8081/overview | grep -o '"taskmanagers":[0-9]*' | cut -d: -f2)
  slots=$(curl -s -m 10 http://192.0.2.64:8081/overview | grep -o '"slots-total":[0-9]*' | cut -d: -f2)
  [ "$tms" = 24 ] && [ "$slots" = 72 ] && break
  sleep 6; t=$((t+6))
done
note "RESTORE_TMS_${tms}_SLOTS_${slots}"

# --- Phase 5: analysis (all labels incl. new tp) ---
$P analysis_scalability.py > analysis_voltp.log 2>&1
note "ANALYSIS_RC_$?"
note "VOLTP_ALL_DONE"
