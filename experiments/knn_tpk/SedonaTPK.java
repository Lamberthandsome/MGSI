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

/** Temporal / point / k-NN queries over the converted raster tiles, one mode list per app. */
public class SedonaTPK {
    static final ObjectMapper JSON = new ObjectMapper();
    static final Set<String> HOSTS = new HashSet<>(Arrays.asList("192.0.2.65", "192.0.2.66", "192.0.2.71", "192.0.2.72"));

    static class Reader extends SparkListener {
        final Set<String> hosts = ConcurrentHashMap.newKeySet();
        final AtomicLong bytes = new AtomicLong();
        public void onTaskEnd(SparkListenerTaskEnd event) {
            if (event.taskMetrics() != null && event.taskMetrics().inputMetrics().bytesRead() > 0
                    && event.taskMetrics().inputMetrics().recordsRead() > 0) {
                try { hosts.add(InetAddress.getByName(event.taskInfo().host()).getHostAddress()); }
                catch (Exception e) { throw new RuntimeException(e); }
                bytes.addAndGet(event.taskMetrics().inputMetrics().bytesRead());
            }
        }
        void clear() { hosts.clear(); bytes.set(0); }
    }

    static Map<String, Integer> allocatedHosts(SparkSession spark) throws Exception {
        Map<String, Integer> counts = new TreeMap<>();
        for (String host : HOSTS) counts.put(host, 0);
        scala.collection.Iterator<String> endpoints = spark.sparkContext().getExecutorMemoryStatus().keysIterator();
        while (endpoints.hasNext()) {
            String endpoint = endpoints.next();
            String host = InetAddress.getByName(endpoint.substring(0, endpoint.lastIndexOf(':'))).getHostAddress();
            if (HOSTS.contains(host)) counts.put(host, counts.get(host) + 1);
            else if (!host.equals("192.0.2.64")) throw new IllegalStateException("Unexpected executor host " + host);
        }
        for (Map.Entry<String, Integer> item : counts.entrySet())
            if (item.getValue() != 4) throw new IllegalStateException("Expected four executors per host: " + counts);
        return counts;
    }

    static double tolerance(double a, double b) { return Math.max(1e-6, 1e-9 * Math.max(Math.abs(a), Math.abs(b))); }

    static void checkTemporal(Map<String, Object> monthly, JsonNode expected, String mode) {
        long count = ((Number) monthly.get("count")).longValue();
        double total = ((Number) monthly.get("sum")).doubleValue();
        if (count != expected.get("count").asLong())
            throw new IllegalStateException("Temporal count mismatch " + mode + " actual=" + count + " expected=" + expected.get("count"));
        if (Math.abs(total - expected.get("sum").asDouble()) > tolerance(total, expected.get("sum").asDouble()))
            throw new IllegalStateException("Temporal sum mismatch " + mode + " actual=" + total + " expected=" + expected.get("sum"));
    }

    static void checkCell(Map<String, Object> observed, JsonNode expected, String what) {
        if (((Number) observed.get("count")).longValue() != expected.get("count").asLong())
            throw new IllegalStateException(what + " count mismatch actual=" + observed + " expected=" + expected);
        double mean = ((Number) observed.get("mean")).doubleValue();
        if (Math.abs(mean - expected.get("mean").asDouble()) > tolerance(mean, expected.get("mean").asDouble()))
            throw new IllegalStateException(what + " mean mismatch actual=" + mean + " expected=" + expected.get("mean").asDouble());
    }

    public static void main(String[] args) throws Exception {
        Path root = Paths.get(args[0]);
        Path manifest = Paths.get(args[1]);
        String dataset = args[2];
        String[] modes = args[3].split(",");
        int measuredLimit = Integer.parseInt(args[4]);
        if (args.length > 5) { HOSTS.clear(); HOSTS.addAll(Arrays.asList(args[5].split(","))); }
        int executors = HOSTS.size() * 4, cores = executors * 3;
        JsonNode inputs = JSON.readTree(manifest.resolve("matrix_inputs.json").toFile()).get(dataset);
        JsonNode preparation = JSON.readTree(manifest.resolve(dataset + "_preparation.json").toFile());
        JsonNode reference = JSON.readTree(root.resolve(dataset + "_tpk_reference.json").toFile());
        JsonNode grid = reference.get("grid");
        double x0 = reference.get("box").get("lon_min").asDouble(), x1 = reference.get("box").get("lon_max").asDouble();
        double y0 = reference.get("box").get("lat_min").asDouble(), y1 = reference.get("box").get("lat_max").asDouble();
        long initialization = System.nanoTime();
        SparkSession spark = SedonaContext.create(SedonaContext.builder().appName("MGSI-tpk-sedona-" + dataset)
            .config("spark.sql.shuffle.partitions", Integer.toString(cores))
            .config("spark.sql.files.maxPartitionBytes", dataset.equals("ncep") ? "1048576" : "67108864")
            .config("spark.sql.files.openCostInBytes", dataset.equals("ncep") ? "0" : "4194304")
            .config("spark.sql.files.minPartitionNum", Integer.toString(cores))
            .config("spark.sql.parquet.filterPushdown", "true").config("spark.sql.adaptive.enabled", "false").getOrCreate());
        Reader reader = new Reader();
        spark.sparkContext().addSparkListener(reader);
        try {
            if (!spark.version().equals("3.5.6")) throw new IllegalStateException("Unexpected Spark version " + spark.version());
            long deadline = System.currentTimeMillis() + 180000;
            while (spark.sparkContext().getExecutorMemoryStatus().size() < executors + 1 && System.currentTimeMillis() < deadline) Thread.sleep(200);
            if (spark.sparkContext().getExecutorMemoryStatus().size() != executors + 1) throw new IllegalStateException("Expected " + executors + " executors");
            double initSeconds = (System.nanoTime() - initialization) / 1e9;
            String sedonaPath = preparation.get("sedona").get("path").asText();
            for (String mode : modes) {
                Path output = root.resolve("sedona_" + mode + "_" + dataset + "_results.jsonl");
                if (Files.exists(output) && Files.exists(root.resolve("sedona_" + mode + "_" + dataset + "_state.json"))) {
                    // Completed in an earlier app run; preserve records and skip.
                    System.out.println("TPK_SEDONA_SKIP_COMPLETE " + dataset + " " + mode);
                    continue;
                }
                if (Files.exists(output)) throw new IllegalStateException("Preserve earlier results: " + output);
                JsonNode expected = mode.startsWith("temporal") ? reference.get("temporal").get(mode) : null;
                List<Double> measured = new ArrayList<>();
                for (int repeat = 0; repeat <= measuredLimit; repeat++) {
                    String label = repeat == 0 ? "warmup" : "measured";
                    reader.clear();
                    long timer = System.nanoTime();
                    Dataset<Row> raw = spark.read().parquet("hdfs://192.0.2.64:9000" + sedonaPath);
                    Dataset<Row> query;
                    if (mode.startsWith("temporal")) {
                        String start = expected.get("window").get(0).asText(), end = expected.get("window").get(1).asText();
                        if (dataset.equals("ncep"))
                            raw = raw.filter(col("months").getItem(0).geq(start).and(col("months").getItem(0).leq(end)));
                        else
                            raw = raw.filter(col("date").geq(start).and(col("date").leq(end)));
                        raw = raw.filter(col("lat_max").geq(y0).and(col("lat_min").leq(y1)).and(col("lon_max").geq(x0)).and(col("lon_min").leq(x1)));
                        Dataset<Row> rasters = raw.selectExpr("RS_FromGeoTiff(content) AS raster", "nodata", "months");
                        Dataset<Row> bands = rasters.selectExpr("raster", "nodata", "posexplode(months) AS (band_index,month)");
                        Dataset<Row> stats = bands.selectExpr("month",
                            "RS_SummaryStatsAll(RS_Clip(RS_SetBandNoDataValue(RS_Band(raster,array(band_index+1)),1,nodata),1," +
                            // Include native centers exactly on integer bounds (same epsilon as the spatial runs).
                            String.format(Locale.ROOT, "ST_SetSRID(ST_PolygonFromEnvelope(%.12f,%.12f,%.12f,%.12f),4326)", x0 - 1e-8, y0 - 1e-8, x1 + 1e-8, y1 + 1e-8) +
                            ",false,nodata,true),1,true) AS stats");
                        query = stats.filter(col("stats").isNotNull().and(col("stats.count").gt(0)))
                            .selectExpr("stats.sum AS total", "CAST(stats.count AS BIGINT) AS n")
                            .agg(sum("total").alias("total"), sum("n").alias("n"));
                    } else {
                        List<JsonNode> cells = new ArrayList<>();
                        if (mode.equals("point")) cells.add(reference.get("point_cell"));
                        else for (JsonNode cell : reference.get("knn_cells")) cells.add(cell);
                        StringBuilder points = new StringBuilder();
                        StringBuilder exprs = new StringBuilder();
                        for (int i = 0; i < cells.size(); i++) {
                            JsonNode cell = cells.get(i);
                            String expr = String.format(Locale.ROOT, "RS_Value(RS_Band(raster,array(band_index+1)),ST_SetSRID(ST_Point(%.12f,%.12f),4326))",
                                cell.get("lon").asDouble(), cell.get("lat").asDouble());
                            if (i > 0) { points.append(','); exprs.append(','); }
                            points.append(expr + " AS v" + i);
                            exprs.append(expr);
                        }
                        // Keep only tiles whose bounds contain the target cells.
                        double latMin = cells.get(0).get("lat").asDouble(), latMax = latMin, lonMin = cells.get(0).get("lon").asDouble(), lonMax = lonMin;
                        for (JsonNode cell : cells) {
                            latMin = Math.min(latMin, cell.get("lat").asDouble()); latMax = Math.max(latMax, cell.get("lat").asDouble());
                            lonMin = Math.min(lonMin, cell.get("lon").asDouble()); lonMax = Math.max(lonMax, cell.get("lon").asDouble());
                        }
                        final double fLatMin = latMin, fLatMax = latMax, fLonMin = lonMin, fLonMax = lonMax;
                        Dataset<Row> rawTiles = raw.filter(col("lat_max").geq(fLatMin).and(col("lat_min").leq(fLatMax)).and(col("lon_max").geq(fLonMin)).and(col("lon_min").leq(fLonMax)));
                        Dataset<Row> rasters = rawTiles.selectExpr("RS_FromGeoTiff(content) AS raster", "nodata", "months");
                        Dataset<Row> bands = rasters.selectExpr("raster", "nodata", "posexplode(months) AS (band_index,month)");
                        if (mode.equals("point")) {
                            Dataset<Row> values = bands.selectExpr("nodata", points.toString())
                                .filter(col("v0").isNotNull().and(col("v0").notEqual(col("nodata"))));
                            query = values.agg(sum("v0").alias("total"), count("v0").alias("n"));
                        } else {
                            Dataset<Row> values = bands.selectExpr("nodata", "array(" + exprs + ") AS vs")
                                .selectExpr("nodata", "posexplode(vs) AS (cell_index,v)")
                                .filter(col("v").isNotNull().and(col("v").notEqual(col("nodata"))));
                            query = values.groupBy("cell_index").agg(sum("v").alias("total"), count("v").alias("n")).orderBy("cell_index");
                        }
                    }
                    String plan = query.queryExecution().executedPlan().toString();
                    List<Row> rows = query.collectAsList();
                    double elapsed = (System.nanoTime() - timer) / 1e9;
                    spark.sparkContext().listenerBus().waitUntilEmpty(10000);
                    // Small point/kNN graphs may not place tasks on every host; actual
                    // participation is recorded per run instead of asserted.
                    if (reader.hosts.isEmpty())
                        throw new IllegalStateException("No data hosts observed");
                    Map<String, Object> record = new LinkedHashMap<>();
                    record.put("status", "PASS"); record.put("method", "sedona"); record.put("dataset", dataset);
                    record.put("mode", mode); record.put("run", label);
                    record.put("until_final_result_seconds", elapsed);
                    record.put("input_metric_bytes_read", reader.bytes.get());
                    record.put("active_data_hosts", new TreeSet<>(reader.hosts));
                    record.put("allocated_executor_hosts", allocatedHosts(spark));
                    record.put("executors", executors); record.put("cores", cores);
                    record.put("declared_worker_hosts", new TreeSet<>(HOSTS));
                    record.put("application_id", spark.sparkContext().applicationId());
                    record.put("initialization_seconds_separate", initSeconds);
                    record.put("executed_plan", plan);
                    record.put("scope", inputs.get("scope").asText());
                    record.put("protocol", "Resident Spark/Sedona over full-time converted raster tiles; one warmup unrecorded; "
                        + measuredLimit + " measured runs; mean and sample standard deviation of the measured runs reported; "
                        + "no persist/result cache; initialization/conversion separate; uncontrolled OS/HDFS caches; complete result return included.");
                    if (mode.startsWith("temporal")) {
                        Row row = rows.get(0);
                        Map<String, Object> result = new LinkedHashMap<>();
                        result.put("sum", row.getDouble(0)); result.put("count", row.getLong(1));
                        result.put("mean", row.getLong(1) > 0 ? row.getDouble(0) / row.getLong(1) : null);
                        record.put("result", result);
                        checkTemporal(result, expected, mode);
                    } else if (mode.equals("point")) {
                        Row row = rows.get(0);
                        JsonNode cell = reference.get("point_cell");
                        Map<String, Object> result = new LinkedHashMap<>();
                        result.put("lat", cell.get("lat").asDouble()); result.put("lon", cell.get("lon").asDouble());
                        result.put("sum", row.getDouble(0)); result.put("count", row.getLong(1));
                        result.put("mean", row.getLong(1) > 0 ? row.getDouble(0) / row.getLong(1) : null);
                        record.put("result", result);
                        checkCell(result, cell, "point");
                    } else {
                        JsonNode cells = JSON.readTree(reference.get("knn_cells").toString());
                        List<Map<String, Object>> results = new ArrayList<>();
                        for (int i = 0; i < rows.size(); i++) {
                            Row row = rows.get(i);
                            if (row.getInt(0) != i) throw new IllegalStateException("kNN cell order " + row.getInt(0));
                            JsonNode cell = cells.get(i);
                            Map<String, Object> result = new LinkedHashMap<>();
                            result.put("lat", cell.get("lat").asDouble()); result.put("lon", cell.get("lon").asDouble());
                            result.put("sum", row.getDouble(1)); result.put("count", row.getLong(2));
                            result.put("mean", row.getLong(2) > 0 ? row.getDouble(1) / row.getLong(2) : null);
                            results.add(result);
                            checkCell(result, cell, "knn cell " + i);
                        }
                        if (results.size() != cells.size()) throw new IllegalStateException("kNN cell count " + results.size());
                        record.put("result", results);
                    }
                    Files.write(output, (JSON.writeValueAsString(record) + "\n").getBytes(StandardCharsets.UTF_8),
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                    System.out.println("TPK_SEDONA_RUN " + dataset + " " + mode + " " + label + " seconds=" + elapsed);
                    if (!label.equals("warmup")) {
                        measured.add(elapsed);
                        if (measured.size() >= measuredLimit) break;
                    }
                }
                double[] values = measured.stream().mapToDouble(Double::doubleValue).toArray();
                double mean = Arrays.stream(values).average().orElse(Double.NaN);
                double variance = values.length > 1
                    ? Arrays.stream(values).map(v -> (v - mean) * (v - mean)).sum() / (values.length - 1) : 0.0;
                double sampleStd = Math.sqrt(variance);
                Map<String, Object> summary = new LinkedHashMap<>();
                summary.put("status", "COMPLETE"); summary.put("dataset", dataset); summary.put("mode", mode);
                summary.put("measured_runs", measured); summary.put("reported_statistic", mean); summary.put("sample_std", sampleStd);
                summary.put("statistic_rule", "mean±sample_std");
                Files.write(root.resolve("sedona_" + mode + "_" + dataset + "_state.json"),
                    JSON.writeValueAsBytes(summary));
                System.out.println("TPK_SEDONA_QUERY_PASS " + dataset + " " + mode + " measured=" + measured + " statistic=" + statistic);
            }
        } finally { spark.stop(); }
    }
}
