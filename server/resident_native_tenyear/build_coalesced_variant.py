"""Create a separate physical-range batching variant; keep prior pins intact."""
from pathlib import Path
import shutil

ROOT=Path(__file__).resolve().parent
target=ROOT/'coalesced'
target.mkdir(exist_ok=True)
source=(ROOT/'ResidentMGSI.java').read_text(encoding='utf-8')
addition='''
        static class Need {
            FileSpec file; int time,rc,cc; Block block; String key;
            Need(FileSpec f,int t,int r,int c){file=f;time=t;rc=r;cc=c;
                int id=((t/f.chunk[0])*f.grid[1]+r)*f.grid[2]+c;
                block=f.physical.get(id);key=f.id+":"+id+":"+t;
                if(block==null)throw new IllegalArgumentException("Physical block absent");}
        }
        synchronized void prefetch(List<Need> needs,Stats stats)throws Exception {
            LinkedHashMap<String,Need> unique=new LinkedHashMap<>();
            for(Need n:needs)if(!planes.containsKey(n.key))unique.put(n.key,n);
            List<Need> pending=new ArrayList<>();
            for(Need n:unique.values()) {
                if(n.block.size==0){float[] fill=new float[n.file.chunk[1]*n.file.chunk[2]];Arrays.fill(fill,(float)n.file.fill);store(n.key,fill);stats.misses++;}
                else pending.add(n);
            }
            pending.sort(Comparator.comparingInt((Need n)->n.file.id).thenComparingLong(n->n.block.offset));
            int current=0;
            while(current<pending.size()) {
                Need first=pending.get(current);FileSpec f=first.file;
                long start=first.block.offset,end=start+first.block.size;int last=current+1;
                while(last<pending.size()) {
                    Need next=pending.get(last);long nextEnd=next.block.offset+next.block.size;
                    if(next.file.id!=f.id||next.block.offset>end+4096||Math.max(end,nextEnd)-start>8L*1024*1024)break;
                    end=Math.max(end,nextEnd);last++;
                }
                if(f.stream==null)f.stream=fs.open(new org.apache.hadoop.fs.Path(f.path));
                byte[] span=new byte[(int)(end-start)];f.stream.readFully(start,span);
                stats.bytes+=span.length;stats.requests++;
                for(int i=current;i<last;i++) {
                    Need need=pending.get(i);Block block=need.block;
                    int offset=(int)(block.offset-start),total=f.chunk[0]*f.chunk[1]*f.chunk[2]*4;
                    byte[] raw;
                    if((block.mask&1)!=0){raw=Arrays.copyOfRange(span,offset,offset+block.size);if(raw.length!=total)throw new IOException("Unfiltered size");}
                    else {
                        raw=new byte[total];Inflater inflater=new Inflater();
                        try{inflater.setInput(span,offset,block.size);int n=0;while(n<total&&!inflater.finished()){int k=inflater.inflate(raw,n,total-n);if(k==0)throw new IOException("Incomplete batched deflate");n+=k;}if(n!=total||!inflater.finished())throw new IOException("Batched decoded size");}
                        finally{inflater.end();}
                    }
                    ByteBuffer decoded=ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN);
                    decoded.position(need.time%f.chunk[0]*f.chunk[1]*f.chunk[2]*4);
                    float[] values=new float[f.chunk[1]*f.chunk[2]];
                    for(int p=0;p<values.length;p++)values[p]=decoded.getFloat();
                    store(need.key,values);stats.misses++;
                }
                current=last;
            }
        }
        void store(String key,float[] values) {
            long size=values.length*4L;
            while(cacheBytes+size>limit&&!planes.isEmpty()){Map.Entry<String,float[]> oldest=planes.entrySet().iterator().next();cacheBytes-=oldest.getValue().length*4L;planes.remove(oldest.getKey());}
            float[] old=planes.put(key,values);if(old!=null)cacheBytes-=old.length*4L;cacheBytes+=size;
        }
'''
anchor='        synchronized float[] plane(FileSpec f,int ti,int rc,int cc,Stats stats)throws Exception{'
assert source.count(anchor)==1
source=source.replace(anchor,addition+'\n'+anchor)
source=source.replace('static class Stats {long bytes=0,misses=0,hits=0;}','static class Stats {long bytes=0,misses=0,hits=0,requests=0;}')
source=source.replace('f.stream.readFully(b.offset,compressed);stats.bytes+=b.size;','f.stream.readFully(b.offset,compressed);stats.bytes+=b.size;stats.requests++;')
prefetch='''
            List<Pool.Need> needed=new ArrayList<>();
            for(JsonNode id:q.get("entries")) {
                Entry e=pool.entries[id.asInt()];FileSpec f=pool.files[e.file];
                int[] b={Integer.MAX_VALUE,-1,Integer.MAX_VALUE,-1};pool.tree(e).query(f,box,b);
                if(b[1]<0)continue;
                for(int ti:f.times)for(int rc=b[0]/f.chunk[1];rc<=(b[1]-1)/f.chunk[1];rc++)
                    for(int cc=b[2]/f.chunk[2];cc<=(b[3]-1)/f.chunk[2];cc++)needed.add(new Pool.Need(f,ti,rc,cc));
            }
            pool.prefetch(needed,stats);
'''
anchor='            Map<String,double[]> sums=new TreeMap<>();Map<String,Long> counts=new TreeMap<>();'
assert source.count(anchor)==1
source=source.replace(anchor,prefetch+'\n'+anchor)
source=source.replace('out.put("physical_read_bytes",stats.bytes);','out.put("physical_read_bytes",stats.bytes);out.put("physical_read_requests",stats.requests);')
(target/'ResidentMGSI.java').write_text(source,encoding='utf-8')
controller=(ROOT/'run_resident.py').read_text(encoding='utf-8')
controller=controller.replace('PORT = 38766','PORT = 38767')
controller=controller.replace('for region in range(1,9):','for region in (1,8):')
controller=controller.replace("        assignment = locality_assignment(meta, connections)","        owner_start=time.perf_counter()\n        assignment = locality_assignment(meta, connections)\n        initialization['ownership_preparation_seconds']=time.perf_counter()-owner_start\n        initialization['until_query_service_ready_seconds']=time.perf_counter()-started\n        (ROOT/'initialization.json').write_text(json.dumps(initialization,indent=2))")
controller=controller.replace("'physical_read_bytes':sum(row['physical_read_bytes'] for row in rows),","'physical_read_bytes':sum(row['physical_read_bytes'] for row in rows),\n                          'physical_read_requests':sum(row['physical_read_requests'] for row in rows),")
controller=controller.replace('Resident Flink/JVM; first query per region clears selected-plane cache, then five repeated queries;', 'Resident Flink/JVM with batched adjacent physical reads; R1/R8; first query per region clears selected-plane cache, then five repeated queries;')
(target/'run_resident.py').write_text(controller,encoding='utf-8')
shutil.copy2(ROOT/'capture_evidence.py',target/'capture_evidence.py')
print('Created isolated coalesced variant; main source and run evidence preserved.')
