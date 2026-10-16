#!/bin/bash
# Scalability chain part 2: tm2/tm4/tm8 + volume slices at 8 TM (tm1 already complete). Leaves 16 TM.
set -u
R=/opt/mgsi-experiments/20261016-scalability
P=/root/miniconda3/bin/python
PY312=/opt/mgsi-modern/python312/bin/python
STATE=/opt/mgsi-experiments/scal_chain.state
JH=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.412.b08-1.el7_9.x86_64
note(){ echo "$(date '+%F %T') $1" >> $STATE; }
cd $R
note "CHAIN2_START"
for N in 2 4 8; do
  bash scale_topology.sh $N >> $STATE 2>&1
  $P -u run_scalability.py --tms $N --label tm$N > run_tm$N.log 2>&1
  note "TM${N}_RC_$?"
done
export JAVA_HOME=$JH
export LD_LIBRARY_PATH=/app/hadoop-2.7.2/lib/native:$JH/jre/lib/amd64/server
export CLASSPATH=/app/hadoop-2.7.2/share/hadoop/hdfs/*:/app/hadoop-2.7.2/share/hadoop/common/*:/app/hadoop-2.7.2/share/hadoop/common/lib/*
for K in 1 5; do
  $PY312 slice_meta_volume.py $K > slice$K.log 2>&1
  note "SLICE${K}_RC_$?"
  M=$R/physical_index_vol${K}yr.json
  for w in 65 66 71 72; do
    ssh -o BatchMode=yes -o StrictHostKeyChecking=yes root@192.0.2.$w "mkdir -p $R"
    scp -o BatchMode=yes -o StrictHostKeyChecking=yes $M root@192.0.2.$w:$M
  done
  $P -u run_scalability.py --tms 8 --label vol${K}yr --meta physical_index_vol${K}yr.json \
      --golden-extra golden_extra_vol${K}yr.json --throughput-seconds 0 > run_vol${K}yr.log 2>&1
  note "VOL${K}_RC_$?"
done
bash scale_topology.sh 8 >> $STATE 2>&1
for w in 65 66 71 72; do ssh -o BatchMode=yes -o StrictHostKeyChecking=yes root@192.0.2.$w 'systemctl start mgsi-flink-taskmanager@3 mgsi-flink-taskmanager@4'; done
sleep 12
note "FINAL_TMS_$(curl -s http://192.0.2.64:8081/overview | grep -o '"taskmanagers":[0-9]*' | cut -d: -f2)"
note "CHAIN2_DONE"
