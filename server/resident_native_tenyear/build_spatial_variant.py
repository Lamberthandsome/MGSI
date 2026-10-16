"""Co-locate neighboring logical tiles to share overlapping physical planes."""
from pathlib import Path
import shutil

ROOT=Path(__file__).resolve().parent
target=ROOT/'spatial_coalesced'
target.mkdir(exist_ok=True)
for name in ('ResidentMGSI.java','capture_evidence.py'):
    shutil.copy2(ROOT/'coalesced'/name,target/name)
source=(ROOT/'coalesced/run_resident.py').read_text(encoding='utf-8')
start=source.index('def locality_assignment(')
end=source.index('\n\ndef main():',start)
function='''def locality_assignment(meta, connections):
    groups=defaultdict(list)
    for route,info in connections.workers.items():
        groups[(info['host'],info['jvm'])].append(route)
    assert len(groups)==24 and all(len(v)==3 for v in groups.values())
    spatial=defaultdict(list)
    for entry in meta['entries']:
        spatial[(entry['file_id'],entry['rows'][0]//450,entry['cols'][0]//720)].append(entry)
    physical=[{int(r[0]):r for r in f['physical_chunks']} for f in meta['files']]
    load={g:0.0 for g in groups}
    route_load={r:0.0 for r in connections.workers}
    assignment={}
    records=[]
    for group_key,entries in sorted(spatial.items()):
        f=meta['files'][group_key[0]]
        pc,pg=f['physical_shape'],f['physical_grid']
        r0=min(e['rows'][0] for e in entries);r1=max(e['rows'][1] for e in entries)
        c0=min(e['cols'][0] for e in entries);c1=max(e['cols'][1] for e in entries)
        weights=defaultdict(float);cost=0;keys=set()
        for ti in f['time_indices']:
            for rc in range(r0//pc[1],(r1-1)//pc[1]+1):
                for cc in range(c0//pc[2],(c1-1)//pc[2]+1):
                    key=(ti//pc[0]*pg[1]+rc)*pg[2]+cc
                    if key in keys:
                        continue
                    keys.add(key)
                    _,offset,size,mask=physical[f['file_id']][key]
                    cost+=size
                    for block in f['hdfs_blocks']:
                        overlap=max(0,min(offset+size,block['offset']+block['length'])-max(offset,block['offset']))
                        if overlap:
                            for host in block['hosts']:
                                weights[host]+=overlap
        cost+=len(keys)*pc[1]*pc[2]*4
        maximum=max(weights.values()) if weights else 0
        choices=[g for g in groups if not weights or weights[g[0]]>=maximum*0.70] or list(groups)
        owner=min(choices,key=lambda g:(load[g],g))
        load[owner]+=cost
        for entry in entries:
            route=min(groups[owner],key=lambda r:(route_load[r],r))
            route_load[route]+=(entry['rows'][1]-entry['rows'][0])*(entry['cols'][1]-entry['cols'][0])*len(f['time_indices'])
            assignment[entry['entry_id']]=route
            records.append({'entry_id':entry['entry_id'],'spatial_group':list(group_key),'route':route,'host':owner[0],'jvm':owner[1],
                            'group_estimated_cost':cost,'local_bytes_weight':weights[owner[0]],'best_local_bytes_weight':maximum})
    (ROOT/'locality_assignment.json').write_text(json.dumps(records,indent=2))
    return assignment
'''
source=source[:start]+function+source[end:]
source=source.replace('PORT = 38767','PORT = 38768')
source=source.replace('for region in (1,8):','for region in range(1,9):')
source=source.replace('with batched adjacent physical reads; R1/R8;', 'with batched adjacent physical reads and neighboring tiles co-located in one JVM; all R1-R8;')
(target/'run_resident.py').write_text(source,encoding='utf-8')
print('Created separate spatial co-location variant; calculation and source values unchanged.')
