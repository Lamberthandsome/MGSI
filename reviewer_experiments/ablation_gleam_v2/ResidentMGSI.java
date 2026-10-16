import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.zip.Inflater;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.*;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.*;
import org.apache.hadoop.fs.*;

/** Resident native-array queries. No Python UDF, query result cache or GROUP BY. */
public class ResidentMGSI {
    static final ObjectMapper JSON = new ObjectMapper();
    static final Map<String,Pool> POOLS = new HashMap<>();

    static class Block {
        long offset; int size,mask;
        Block(JsonNode row) { offset=row.get(1).asLong();size=row.get(2).asInt();mask=row.get(3).asInt(); }
    }
    static class FileSpec {
        String path; int id; int[] shape,chunk,grid,times; String[] labels;
        double[] lat,lon; double fill,scale,offset;
        Map<Integer,Block> physical=new HashMap<>(); FSDataInputStream stream;
        FileSpec(JsonNode j) {
            path=j.get("path").asText(); id=j.get("file_id").asInt();
            shape=ints(j.get("shape"));chunk=ints(j.get("physical_shape"));grid=ints(j.get("physical_grid"));
            times=ints(j.get("time_indices"));labels=new String[times.length];
            for(int i=0;i<labels.length;i++)labels[i]=j.get("time_labels").get(i).asText().substring(0,7);
            lat=doubles(j.get("lat"));lon=doubles(j.get("lon"));
            fill=j.get("fill").asDouble();scale=j.get("scale").asDouble();offset=j.get("offset").asDouble();
            if(!j.get("dtype").asText().equals("<f4"))throw new IllegalArgumentException("Unsupported dtype");
            for(JsonNode filter:j.get("filters"))if(filter.asInt()!=1)throw new IllegalArgumentException("Unsupported filter");
            for(JsonNode row:j.get("physical_chunks"))physical.put(row.get(0).asInt(),new Block(row));
        }
    }
    static int[] ints(JsonNode j){int[] a=new int[j.size()];for(int i=0;i<a.length;i++)a[i]=j.get(i).asInt();return a;}
    static double[] doubles(JsonNode j){double[] a=new double[j.size()];for(int i=0;i<a.length;i++)a[i]=j.get(i).asDouble();return a;}
    static class Entry {
        int id,file,r0,r1,c0,c1;
        Entry(JsonNode j){id=j.get("entry_id").asInt();file=j.get("file_id").asInt();r0=j.get("rows").get(0).asInt();r1=j.get("rows").get(1).asInt();c0=j.get("cols").get(0).asInt();c1=j.get("cols").get(1).asInt();}
    }
    /** KD spatial partition of regular grid points; leaves keep row/column ranges. */
    static class KD {
        int r0,r1,c0,c1; double ymin,ymax,xmin,xmax; KD left,right;
        KD(FileSpec f,int a,int b,int c,int d,int depth){
            r0=a;r1=b;c0=c;c1=d;
            ymin=Math.min(f.lat[a],f.lat[b-1]);ymax=Math.max(f.lat[a],f.lat[b-1]);
            xmin=Math.min(f.lon[c],f.lon[d-1]);xmax=Math.max(f.lon[c],f.lon[d-1]);
            if((b-a)*(d-c)>64){
                if((depth%2==0&&d-c>1)||b-a==1){int m=(c+d)/2;left=new KD(f,a,b,c,m,depth+1);right=new KD(f,a,b,m,d,depth+1);}
                else {int m=(a+b)/2;left=new KD(f,a,m,c,d,depth+1);right=new KD(f,m,b,c,d,depth+1);}
            }
        }
        void query(FileSpec f,double[] box,int[] bounds){
            if(ymax<box[0]||ymin>box[1]||xmax<box[2]||xmin>box[3])return;
            if(ymin>=box[0]&&ymax<=box[1]&&xmin>=box[2]&&xmax<=box[3]){merge(bounds,r0,r1,c0,c1);return;}
            if(left!=null){left.query(f,box,bounds);right.query(f,box,bounds);return;}
            for(int r=r0;r<r1;r++)if(f.lat[r]>=box[0]&&f.lat[r]<=box[1])
                for(int c=c0;c<c1;c++)if(f.lon[c]>=box[2]&&f.lon[c]<=box[3])merge(bounds,r,r+1,c,c+1);
        }
        static void merge(int[] b,int a,int z,int c,int d){b[0]=Math.min(b[0],a);b[1]=Math.max(b[1],z);b[2]=Math.min(b[2],c);b[3]=Math.max(b[3],d);}
    }
    static class Stats {long bytes=0,misses=0,hits=0,requests=0;}
    static class Pool {
        FileSpec[] files; Entry[] entries;
        Map<String,KD> trees=new HashMap<>();
        LinkedHashMap<String,float[]> planes=new LinkedHashMap<>(256,0.75f,true);
        long cacheBytes=0,limit=512L*1024*1024,preparedHeaderBytes=0; int users=0; String epoch="";
        org.apache.hadoop.fs.FileSystem fs;
        Pool(String path)throws Exception{
            JsonNode meta=JSON.readTree(new File(path));
            fs=org.apache.hadoop.fs.FileSystem.get(new URI("hdfs://192.0.2.64:9000"),new org.apache.hadoop.conf.Configuration());
            files=new FileSpec[meta.get("files").size()];for(JsonNode j:meta.get("files")){FileSpec f=new FileSpec(j);files[f.id]=f;}
            entries=new Entry[meta.get("entries").size()];for(JsonNode j:meta.get("entries")){Entry e=new Entry(j);entries[e.id]=e;}
            for(FileSpec f:files) {
                f.stream=fs.open(new org.apache.hadoop.fs.Path(f.path));
                byte[] signature=new byte[8];f.stream.readFully(0,signature);
                byte[] hdf={(byte)137,72,68,70,13,10,26,10};
                if(!Arrays.equals(signature,hdf))throw new IOException("Invalid HDF5 signature");
                preparedHeaderBytes+=signature.length;
            }
            for(Entry e:entries)tree(e);

        }
        synchronized KD tree(Entry e){return treeFor(files[e.file],e.r0,e.r1,e.c0,e.c1);}
        synchronized KD treeFor(FileSpec f,int a,int b,int c,int d){
            String key=a+":"+b+":"+c+":"+d;
            KD t=trees.get(key);if(t==null){t=new KD(f,a,b,c,d,0);trees.put(key,t);}return t;
        }
        /** Ablation variant "bounds_mode":"linear" — coordinate scan instead of the prepared KD tree. */
        static void linearBounds(FileSpec f,int a,int b,int c,int d,double[] box,int[] out){
            for(int r=a;r<b;r++)if(f.lat[r]>=box[0]&&f.lat[r]<=box[1])
                for(int col=c;col<d;col++)if(f.lon[col]>=box[2]&&f.lon[col]<=box[3])KD.merge(out,r,r+1,col,col+1);
        }
        synchronized void clear(String next){if(!next.equals(epoch)){planes.clear();cacheBytes=0;epoch=next;}}

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

        synchronized float[] plane(FileSpec f,int ti,int rc,int cc,Stats stats)throws Exception{
            int physical=((ti/f.chunk[0])*f.grid[1]+rc)*f.grid[2]+cc;
            String key=f.id+":"+physical+":"+ti;
            float[] values=planes.get(key);
            if(values!=null){stats.hits++;return values;}
            stats.misses++;
            values=readDecode(f,ti,physical,stats);
            long size=values.length*4L;
            while(cacheBytes+size>limit&&!planes.isEmpty()){Map.Entry<String,float[]> eldest=planes.entrySet().iterator().next();cacheBytes-=eldest.getValue().length*4L;planes.remove(eldest.getKey());}
            planes.put(key,values);cacheBytes+=size;return values;
        }
        /** Ablation variant "cache_mode":"none" — per-block read, no plane cache, no batched prefetch. */
        synchronized float[] planeDirect(FileSpec f,int ti,int rc,int cc,Stats stats)throws Exception{
            stats.misses++;
            int physical=((ti/f.chunk[0])*f.grid[1]+rc)*f.grid[2]+cc;
            return readDecode(f,ti,physical,stats);
        }
        private float[] readDecode(FileSpec f,int ti,int physical,Stats stats)throws Exception{
            Block b=f.physical.get(physical);if(b==null)throw new IOException("Missing physical index "+f.id+":"+physical+":"+ti);
            int width=f.chunk[2],height=f.chunk[1];float[] values=new float[width*height];
            if(b.size==0){Arrays.fill(values,(float)f.fill);}
            else {
                if(f.stream==null)f.stream=fs.open(new org.apache.hadoop.fs.Path(f.path));
                byte[] compressed=new byte[b.size];f.stream.readFully(b.offset,compressed);stats.bytes+=b.size;stats.requests++;
                int total=f.chunk[0]*height*width*4;
                byte[] raw;
                if((b.mask&1)!=0){raw=compressed;if(raw.length!=total)throw new IOException("Invalid unfiltered chunk");}
                else {
                    raw=new byte[total];Inflater inflater=new Inflater();
                    try{inflater.setInput(compressed);int n=0;while(n<total&&!inflater.finished()){int k=inflater.inflate(raw,n,total-n);if(k==0)throw new IOException("Incomplete deflate");n+=k;}if(n!=total||!inflater.finished())throw new IOException("Unexpected chunk length");}
                    finally{inflater.end();}
                }
                ByteBuffer decoded=ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN);
                decoded.position(ti%f.chunk[0]*width*height*4);
                for(int i=0;i<values.length;i++)values[i]=decoded.getFloat();
            }
            return values;
        }
    }
    static class RequestSource extends RichSourceFunction<Tuple2<Integer,String>> {
        final String host;final int port;volatile boolean running=true;transient Socket socket;
        RequestSource(String h,int p){host=h;port=p;}
        public void run(SourceContext<Tuple2<Integer,String>> context)throws Exception{
            socket=new Socket(host,port);socket.setTcpNoDelay(true);
            PrintWriter writer=new PrintWriter(new OutputStreamWriter(socket.getOutputStream(),StandardCharsets.UTF_8),true);
            writer.println("{\"type\":\"source\"}");
            BufferedReader input=new BufferedReader(new InputStreamReader(socket.getInputStream(),StandardCharsets.UTF_8));
            String line;while(running&&(line=input.readLine())!=null){JsonNode j=JSON.readTree(line);if(j.get("type").asText().equals("stop"))break;
                synchronized(context.getCheckpointLock()){context.collect(Tuple2.of(j.get("route").asInt(),line));}}
        }
        public void cancel(){running=false;try{if(socket!=null)socket.close();}catch(Exception ignored){}}
    }
    static class DirectPartition implements Partitioner<Integer>{public int partition(Integer key,int n){if(key<0||key>=n)throw new IllegalArgumentException("route");return key;}}
    static class NativeQuery extends RichMapFunction<Tuple2<Integer,String>,String>{
        final String path;transient Pool pool;
        NativeQuery(String p){path=p;}
        public void open(Configuration config)throws Exception{synchronized(POOLS){pool=POOLS.get(path);if(pool==null){pool=new Pool(path);POOLS.put(path,pool);}pool.users++;}}
        public void close()throws Exception{synchronized(POOLS){if(pool!=null&&--pool.users==0){for(FileSpec f:pool.files)if(f.stream!=null)f.stream.close();pool.planes.clear();pool.trees.clear();POOLS.remove(path);}}}
        public String map(Tuple2<Integer,String> input)throws Exception{
            long start=System.nanoTime();JsonNode q=JSON.readTree(input.f1);
            int route=getRuntimeContext().getIndexOfThisSubtask();if(route!=input.f0)throw new IOException("Partition route mismatch");
            String clear=q.path("clear_epoch").asText();if(!clear.isEmpty())pool.clear(clear);
            double[] box=doubles(q.get("box"));Stats stats=new Stats();

            // Ablation variant flags; absent fields keep the default (full MGSI-F) behaviour.
            boolean fileGranular=q.path("entry_mode").asText("").equals("file");
            boolean linear=q.path("bounds_mode").asText("").equals("linear");
            boolean noCache=q.path("cache_mode").asText("").equals("none");

            List<Pool.Need> needed=new ArrayList<>();
            for(JsonNode id:q.get("entries")) {
                FileSpec f;int er0,er1,ec0,ec1;
                if(fileGranular){f=pool.files[id.asInt()];er0=0;er1=f.shape[1];ec0=0;ec1=f.shape[2];}
                else{Entry e=pool.entries[id.asInt()];f=pool.files[e.file];er0=e.r0;er1=e.r1;ec0=e.c0;ec1=e.c1;}
                int[] b={Integer.MAX_VALUE,-1,Integer.MAX_VALUE,-1};
                if(linear)Pool.linearBounds(f,er0,er1,ec0,ec1,box,b);else pool.treeFor(f,er0,er1,ec0,ec1).query(f,box,b);
                if(b[1]<0)continue;
                if(fileGranular)for(int ti:f.times)for(int rc=0;rc<f.grid[1];rc++)for(int cc=0;cc<f.grid[2];cc++)needed.add(new Pool.Need(f,ti,rc,cc));
                else for(int ti:f.times)for(int rc=b[0]/f.chunk[1];rc<=(b[1]-1)/f.chunk[1];rc++)
                    for(int cc=b[2]/f.chunk[2];cc<=(b[3]-1)/f.chunk[2];cc++)needed.add(new Pool.Need(f,ti,rc,cc));
            }
            if(!noCache)pool.prefetch(needed,stats);

            Map<String,double[]> sums=new TreeMap<>();Map<String,Long> counts=new TreeMap<>();
            for(JsonNode id:q.get("entries")){
                FileSpec f;int er0,er1,ec0,ec1;
                if(fileGranular){f=pool.files[id.asInt()];er0=0;er1=f.shape[1];ec0=0;ec1=f.shape[2];}
                else{Entry e=pool.entries[id.asInt()];f=pool.files[e.file];er0=e.r0;er1=e.r1;ec0=e.c0;ec1=e.c1;}
                int[] bounds={Integer.MAX_VALUE,-1,Integer.MAX_VALUE,-1};
                if(linear)Pool.linearBounds(f,er0,er1,ec0,ec1,box,bounds);else pool.treeFor(f,er0,er1,ec0,ec1).query(f,box,bounds);
                if(bounds[1]<0)continue;
                for(int mi=0;mi<f.times.length;mi++){
                    int ti=f.times[mi];String month=f.labels[mi];double[] sum=sums.computeIfAbsent(month,k->new double[2]);long count=counts.getOrDefault(month,0L);
                    for(int rc=bounds[0]/f.chunk[1];rc<=(bounds[1]-1)/f.chunk[1];rc++){
                        int a=Math.max(bounds[0],rc*f.chunk[1]),b=Math.min(bounds[1],(rc+1)*f.chunk[1]);
                        for(int cc=bounds[2]/f.chunk[2];cc<=(bounds[3]-1)/f.chunk[2];cc++){
                            int c=Math.max(bounds[2],cc*f.chunk[2]),d=Math.min(bounds[3],(cc+1)*f.chunk[2]);
                            float[] plane=noCache?pool.planeDirect(f,ti,rc,cc,stats):pool.plane(f,ti,rc,cc,stats);
                            for(int r=a;r<b;r++){int offset=(r-rc*f.chunk[1])*f.chunk[2]+c-cc*f.chunk[2];
                                for(int col=c;col<d;col++){
                                    float value=plane[offset++];if(!Float.isFinite(value)||value==f.fill)continue;
                                    double y=value*f.scale+f.offset-sum[1],t=sum[0]+y;sum[1]=(t-sum[0])-y;sum[0]=t;count++;
                                }
                            }
                        }
                    }
                    counts.put(month,count);
                }
            }
            ObjectNode out=JSON.createObjectNode();out.put("type","result");out.put("query_id",q.get("query_id").asText());out.put("route",route);
            out.put("entry_mode",fileGranular?"file":"tile");out.put("bounds_mode",linear?"linear":"tree");out.put("cache_mode",noCache?"none":"plane");
            out.put("host",InetAddress.getLocalHost().getHostAddress());out.put("worker_seconds",(System.nanoTime()-start)/1e9);
            out.put("prepared_header_bytes_per_jvm",pool.preparedHeaderBytes);out.put("prepared_spatial_tree_templates_per_jvm",pool.trees.size());
            out.put("physical_read_bytes",stats.bytes);out.put("physical_read_requests",stats.requests);out.put("plane_cache_hits",stats.hits);out.put("plane_cache_misses",stats.misses);
            ObjectNode monthly=out.putObject("monthly");for(String month:sums.keySet()){ObjectNode m=monthly.putObject(month);m.put("sum",sums.get(month)[0]);m.put("count",counts.get(month));}
            return out.toString();
        }
    }
    static class ResultSink extends RichSinkFunction<String>{
        final String host;final int port;transient Socket socket;transient PrintWriter writer;
        ResultSink(String h,int p){host=h;port=p;}
        public void open(Configuration cfg)throws Exception{socket=new Socket(host,port);socket.setTcpNoDelay(true);writer=new PrintWriter(new OutputStreamWriter(socket.getOutputStream(),StandardCharsets.UTF_8),true);
            ObjectNode ready=JSON.createObjectNode();ready.put("type","worker_ready");ready.put("route",getRuntimeContext().getIndexOfThisSubtask());ready.put("host",InetAddress.getLocalHost().getHostAddress());ready.put("jvm",java.lang.management.ManagementFactory.getRuntimeMXBean().getName());writer.println(ready.toString());}
        public void invoke(String value,Context context)throws Exception{writer.println(value);if(writer.checkError())throw new IOException("Result connection failed");}
        public void close()throws Exception{if(socket!=null)socket.close();}
    }
    static void blockmap(String input,String output)throws Exception{
        ObjectNode meta=(ObjectNode)JSON.readTree(new File(input));
        org.apache.hadoop.fs.FileSystem fs=org.apache.hadoop.fs.FileSystem.get(new URI("hdfs://192.0.2.64:9000"),new org.apache.hadoop.conf.Configuration());
        for(JsonNode file:meta.get("files")){
            org.apache.hadoop.fs.Path p=new org.apache.hadoop.fs.Path(file.get("path").asText());FileStatus status=fs.getFileStatus(p);
            if(status.getLen()!=file.get("source_bytes").asLong())throw new IOException("Source changed");
            ArrayNode blocks=((ObjectNode)file).putArray("hdfs_blocks");for(BlockLocation b:fs.getFileBlockLocations(status,0,status.getLen())){
                ObjectNode row=blocks.addObject();row.put("offset",b.getOffset());row.put("length",b.getLength());ArrayNode hosts=row.putArray("hosts");for(String h:b.getHosts())hosts.add(InetAddress.getByName(h).getHostAddress());}
        }
        JSON.writeValue(new File(output),meta);fs.close();
    }
    public static void main(String[] args)throws Exception{
        if(args[0].equals("blockmap")){blockmap(args[1],args[2]);return;}
        String host=args[0],metadata=args[2];int port=Integer.parseInt(args[1]);
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();env.setParallelism(48);env.setBufferTimeout(0);env.setRestartStrategy(RestartStrategies.noRestart());
        env.addSource(new RequestSource(host,port),TypeInformation.of(new TypeHint<Tuple2<Integer,String>>(){})).name("MGSI archived-query requests").setParallelism(1)
            .partitionCustom(new DirectPartition(),0)
            .map(new NativeQuery(metadata)).name("MGSI native physical-index and cached KD query").setParallelism(48)
            .addSink(new ResultSink(host,port)).name("MGSI complete result return").setParallelism(48);
        env.execute("MGSI ablation variants gleam 20261015 v2");
    }
}
