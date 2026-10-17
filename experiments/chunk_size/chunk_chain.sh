#!/bin/bash
# Chunk-size chain: five NCEP re-chunked variants at 16 TM (1 subtask each), latency 1+5.
set -u
R=/opt/mgsi-experiments/chunk_size
P=/root/miniconda3/bin/python
STATE=/opt/mgsi-experiments/chunk_chain.state
note(){ echo "$(date '+%F %T') $1" >> $STATE; }
cd $R
note "CHAIN_START"
for v in plane half quarter time12 time4; do
  $P -u run_scalability.py --tms 16 --slots-per-tm 3 --label $v --meta physical_index_${v}_with_blocks.json \
      --golden-extra golden_extra_${v}.json --throughput-seconds 0 --port 38900 > run_$v.log 2>&1
  note "${v}_RC_$?"
done
note "CHAIN_DONE"
