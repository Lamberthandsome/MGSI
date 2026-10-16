"""Initialize shared metadata/geometry indexes and file connections before queries."""
from pathlib import Path
import shutil

ROOT=Path(__file__).resolve().parent
target=ROOT/'preinitialized'
target.mkdir(exist_ok=True)
source=(ROOT/'spatial_coalesced/ResidentMGSI.java').read_text(encoding='utf-8')
source=source.replace('long cacheBytes=0,limit=512L*1024*1024; String epoch="";', 'long cacheBytes=0,limit=512L*1024*1024,preparedHeaderBytes=0; int users=0; String epoch="";')
anchor='            entries=new Entry[meta.get("entries").size()];for(JsonNode j:meta.get("entries")){Entry e=new Entry(j);entries[e.id]=e;}'
assert source.count(anchor)==1
source=source.replace(anchor,anchor+'''
            for(FileSpec f:files) {
                f.stream=fs.open(new org.apache.hadoop.fs.Path(f.path));
                byte[] signature=new byte[8];f.stream.readFully(0,signature);
                byte[] hdf={(byte)137,72,68,70,13,10,26,10};
                if(!Arrays.equals(signature,hdf))throw new IOException("Invalid HDF5 signature");
                preparedHeaderBytes+=signature.length;
            }
            for(Entry e:entries)tree(e);
''')
anchor='public void open(Configuration config)throws Exception{synchronized(POOLS){pool=POOLS.get(path);if(pool==null){pool=new Pool(path);POOLS.put(path,pool);}}}'
assert source.count(anchor)==1
source=source.replace(anchor,'''public void open(Configuration config)throws Exception{synchronized(POOLS){pool=POOLS.get(path);if(pool==null){pool=new Pool(path);POOLS.put(path,pool);}pool.users++;}}
        public void close()throws Exception{synchronized(POOLS){if(pool!=null&&--pool.users==0){for(FileSpec f:pool.files)if(f.stream!=null)f.stream.close();pool.planes.clear();pool.trees.clear();POOLS.remove(path);}}}''')
source=source.replace('out.put("worker_seconds",(System.nanoTime()-start)/1e9);','out.put("worker_seconds",(System.nanoTime()-start)/1e9);')
anchor='            out.put("physical_read_bytes",stats.bytes);'
assert source.count(anchor)==1
source=source.replace(anchor,'            out.put("prepared_header_bytes_per_jvm",pool.preparedHeaderBytes);out.put("prepared_spatial_tree_templates_per_jvm",pool.trees.size());\n'+anchor)
(target/'ResidentMGSI.java').write_text(source,encoding='utf-8')
controller=(ROOT/'spatial_coalesced/run_resident.py').read_text(encoding='utf-8')
controller=controller.replace('PORT = 38768','PORT = 38769')
controller=controller.replace("        connections.results('initialization_barrier')", "        initial_rows=connections.results('initialization_barrier')\n        assert all(r['prepared_header_bytes_per_jvm']==80 and r['prepared_spatial_tree_templates_per_jvm']==500 for r in initial_rows)")
controller=controller.replace("'workers': connections.workers,", "'workers': connections.workers, 'header_bytes_read_in_initialization':24*80, 'spatial_templates_prepared_per_jvm':500,")
controller=controller.replace('Resident Flink/JVM with batched adjacent physical reads and neighboring tiles co-located in one JVM;', 'Resident Flink/JVM with prepared metadata/geometry indexes and file connections (8-byte signatures only, no variable data); batched adjacent physical reads and neighboring tiles co-located in one JVM;')
(target/'run_resident.py').write_text(controller,encoding='utf-8')
shutil.copy2(ROOT/'capture_evidence.py',target/'capture_evidence.py')
print('Created isolated initialization variant; no query data or aggregates prefetched.')
