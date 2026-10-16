import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.sedona.spark.SedonaContext;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import static org.apache.spark.sql.functions.*;

public class SedonaMatrix {
    static final ObjectMapper JSON=new ObjectMapper();
    static final Set<String> HOSTS=new HashSet<>(Arrays.asList("192.0.2.65","192.0.2.66","192.0.2.71","192.0.2.72"));
    static class Reader extends SparkListener {
        final Set<String> hosts=ConcurrentHashMap.newKeySet();
        final AtomicLong bytes=new AtomicLong();
        public void onTaskEnd(SparkListenerTaskEnd event) {
            if(event.taskMetrics()!=null && event.taskMetrics().inputMetrics().bytesRead()>0
                    && event.taskMetrics().inputMetrics().recordsRead()>0) {
                try { hosts.add(InetAddress.getByName(event.taskInfo().host()).getHostAddress()); }
                catch(Exception e) { throw new RuntimeException(e); }
                bytes.addAndGet(event.taskMetrics().inputMetrics().bytesRead());
            }
        }
        void clear() { hosts.clear(); bytes.set(0); }
    }
    static void check(JsonNode observed,JsonNode expected) {
        if(observed.size()!=120 || expected.size()!=120) throw new IllegalStateException("Expected 120 monthly results.");
        Iterator<String> months=expected.fieldNames();
        while(months.hasNext()) {
            String month=months.next(); JsonNode a=observed.get(month),e=expected.get(month);
            if(a==null || a.get("count").asLong()!=e.get("count").asLong())
                throw new IllegalStateException("Count mismatch "+month+" actual="+a+" expected="+e);
            double total=a.get("sum").asDouble(),goal=e.get("sum").asDouble();
            if(Math.abs(total-goal)>Math.max(1e-6,1e-9*Math.max(Math.abs(total),Math.abs(goal))))
                throw new IllegalStateException("Sum mismatch "+month+" actual="+a+" expected="+e);
        }
    }
    public static void main(String[] args) throws Exception {
        Path root=Paths.get(args[0]); String[] datasets=args[1].split(","),regions=args[2].split(",");
        int repeats=Integer.parseInt(args[3]);
        if(args.length>4) { HOSTS.clear(); HOSTS.addAll(Arrays.asList(args[4].split(","))); }
        if(HOSTS.size()!=2 && HOSTS.size()!=4) throw new IllegalStateException("Declared worker profile must have two or four hosts.");
        int executors=HOSTS.size()*4,cores=executors*3;
        JsonNode boxes=JSON.readTree(root.resolve("boxes.json").toFile());
        JsonNode inputs=JSON.readTree(root.resolve("matrix_inputs.json").toFile());
        long initialization=System.nanoTime();
        SparkSession spark=SedonaContext.create(SedonaContext.builder().appName("MGSI-modern-Sedona-"+HOSTS.size()+"-workers-20261007")
            .config("spark.sql.shuffle.partitions",Integer.toString(cores)).config("spark.sql.files.maxPartitionBytes","67108864")
            .config("spark.sql.parquet.filterPushdown","true").config("spark.sql.adaptive.enabled","false").getOrCreate());
        Reader reader=new Reader(); spark.sparkContext().addSparkListener(reader);
        try {
            if(!spark.version().equals("3.5.6")) throw new IllegalStateException("Unexpected Spark version");
            long deadline=System.currentTimeMillis()+180000;
            while(spark.sparkContext().getExecutorMemoryStatus().size()<executors+1 && System.currentTimeMillis()<deadline) Thread.sleep(200);
            if(spark.sparkContext().getExecutorMemoryStatus().size()!=executors+1) throw new IllegalStateException("Expected "+executors+" executors");
            double initSeconds=(System.nanoTime()-initialization)/1e9;
            Map<String,Object> init=new LinkedHashMap<>();
            init.put("seconds_separate",initSeconds); init.put("application_id",spark.sparkContext().applicationId());
            init.put("spark_version",spark.version()); init.put("sedona_version","1.9.1"); init.put("executors",executors);
            init.put("cores",cores); init.put("declared_worker_hosts",new TreeSet<>(HOSTS));
            Files.write(root.resolve("sedona_initialization.json"),JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(init));
            for(String dataset:datasets) {
                // The NCEP raster archive is only about 52MiB. Split files into
                // enough real input tasks to use all declared workers.
                spark.conf().set("spark.sql.files.maxPartitionBytes",dataset.equals("ncep") ? "1048576" : "67108864");
                spark.conf().set("spark.sql.files.openCostInBytes",dataset.equals("ncep") ? "0" : "4194304");
                spark.conf().set("spark.sql.files.minPartitionNum",Integer.toString(cores));
                Path output=root.resolve("sedona_"+dataset+"_results.jsonl");
                if(Files.exists(output)) throw new IllegalStateException("Preserve earlier results: "+output);
                JsonNode reference=JSON.readTree(root.resolve(dataset+"_reference.json").toFile());
                JsonNode preparation=dataset.equals("chirps") ? null : JSON.readTree(root.resolve(dataset+"_preparation.json").toFile());
                List<String> paths=new ArrayList<>();
                if(dataset.equals("chirps"))
                    for(JsonNode file:inputs.get(dataset).get("files")) paths.add("hdfs://192.0.2.64:9000"+file.get("path").asText());
                for(String regionLabel:regions) {
                    int region=Integer.parseInt(regionLabel); JsonNode box=boxes.get(regionLabel);
                    // Include native centers exactly on integer bounds, without selecting an extra adjacent center.
                    double eps=dataset.equals("chirps") ? 0 : 1e-8;
                    double x0=box.get("lon_min").asDouble(),y0=box.get("lat_min").asDouble();
                    double x1=box.get("lon_max").asDouble(),y1=box.get("lat_max").asDouble();
                    String zone=String.format(Locale.ROOT,"ST_SetSRID(ST_PolygonFromEnvelope(%.12f,%.12f,%.12f,%.12f),4326)",x0-eps,y0-eps,x1+eps,y1+eps);
                    for(int repeat=1;repeat<=repeats;repeat++) {
                        reader.clear(); long timer=System.nanoTime(); Dataset<Row> stats;
                        if(dataset.equals("chirps")) {
                            Dataset<Row> raw=spark.read().format("binaryFile").load(paths.toArray(new String[0]));
                            stats=raw.selectExpr("concat(regexp_extract(path,'chirps-v2[.]0[.](....)[.](..)[.](..)[.]tif$',1),'-',regexp_extract(path,'chirps-v2[.]0[.](....)[.](..)[.](..)[.]tif$',2)) AS month",
                                "RS_SummaryStatsAll(RS_Clip(RS_SetBandNoDataValue(RS_FromGeoTiff(content),1,CAST(-9999 AS DOUBLE)),1,"+zone+",false,CAST(-9999 AS DOUBLE),true),1,true) AS stats");
                        } else {
                            Dataset<Row> raw=spark.read().parquet("hdfs://192.0.2.64:9000"+preparation.get("sedona").get("path").asText());
                            if(!dataset.equals("ncep")) raw=raw.filter(col("day").equalTo(15));
                            raw=raw.filter(col("lat_max").geq(y0).and(col("lat_min").leq(y1)).and(col("lon_max").geq(x0)).and(col("lon_min").leq(x1)));
                            Dataset<Row> rasters=raw.selectExpr("RS_FromGeoTiff(content) AS raster","nodata","months");
                            Dataset<Row> bands=rasters.selectExpr("raster","nodata","posexplode(months) AS (band_index,month)");
                            stats=bands.selectExpr("month","RS_SummaryStatsAll(RS_Clip(RS_SetBandNoDataValue(RS_Band(raster,array(band_index+1)),1,nodata),1,"+zone+",false,nodata,true),1,true) AS stats");
                        }
                        Dataset<Row> query=stats.filter(col("stats").isNotNull().and(col("stats.count").gt(0)))
                            .selectExpr("month","stats.sum AS total","CAST(stats.count AS BIGINT) AS n")
                            .groupBy("month").agg(sum("total").alias("total"),sum("n").alias("n"));
                        String plan=query.queryExecution().executedPlan().toString();
                        List<Row> values=query.collectAsList(); Map<String,Object> monthly=new TreeMap<>();
                        for(Row row:values) {
                            String month=row.getString(0); double total=((Number)row.getAs("total")).doubleValue();
                            long count=((Number)row.getAs("n")).longValue(); Map<String,Object> value=new LinkedHashMap<>();
                            value.put("sum",total); value.put("count",count); value.put("mean",count>0 ? total/count : null);
                            if(monthly.put(month,value)!=null) throw new IllegalStateException("Duplicate month "+month);
                        }
                        double elapsed=(System.nanoTime()-timer)/1e9;
                        check(JSON.valueToTree(monthly),reference.get(regionLabel));
                        spark.sparkContext().listenerBus().waitUntilEmpty(10000);
                        if(!reader.hosts.equals(HOSTS)) throw new IllegalStateException("Data worker participation "+reader.hosts);
                        Map<String,Object> record=new LinkedHashMap<>();
                        record.put("status","PASS"); record.put("method","sedona"); record.put("dataset",dataset);
                        record.put("region",region); record.put("repeat",repeat);
                        record.put("mode",repeat==1 ? "warmup" : "measured"); record.put("measured_repeat",repeat-1); record.put("application_id",spark.sparkContext().applicationId());
                        record.put("until_final_120_month_results_seconds",elapsed); record.put("monthly_values",monthly);
                        record.put("active_data_hosts",new TreeSet<>(reader.hosts)); record.put("input_metric_bytes_read",reader.bytes.get());
                        record.put("executors",executors); record.put("cores",cores); record.put("declared_worker_hosts",new TreeSet<>(HOSTS));
                        record.put("cluster_profile",HOSTS.size()==4 ? "matched_four_worker_batch" : "two_worker_functional_pilot"); record.put("executed_plan",plan);
                        record.put("source_max_partition_bytes",spark.conf().get("spark.sql.files.maxPartitionBytes"));
                        record.put("source_open_cost_bytes",spark.conf().get("spark.sql.files.openCostInBytes"));
                        record.put("initialization_seconds_separate",initSeconds); record.put("scope",inputs.get(dataset).get("scope").asText());
                        record.put("protocol","Resident Spark/Sedona, native TIFF or full-time converted raster tiles; no persist/result cache; initialization/conversion separate; uncontrolled OS/HDFS caches; complete monthly result return included.");
                        Files.write(output,(JSON.writeValueAsString(record)+"\n").getBytes(StandardCharsets.UTF_8),StandardOpenOption.CREATE,StandardOpenOption.APPEND);
                        System.out.println("MATRIX_SEDONA_QUERY_PASS "+dataset+" R"+region+" repeat="+repeat+" seconds="+elapsed);
                    }
                }
            }
            Files.write(root.resolve("sedona_state.json"),JSON.writeValueAsBytes(Collections.singletonMap("status","COMPLETE")));
        } finally { spark.stop(); }
    }
}
