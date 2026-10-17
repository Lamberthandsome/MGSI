import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.*;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.*;

/** Local Flink integration check, using only metadata and echo sinks, without HDFS. */
public final class JobManagerGlobalIndexSourceCheck {
    private static final ObjectMapper JSON = new ObjectMapper();

    public static final class EchoSink extends RichSinkFunction<Tuple2<Integer, String>> {
        private final String host;
        private final int port;
        private transient Socket socket;
        private transient PrintWriter output;
        EchoSink(String host, int port) { this.host = host; this.port = port; }
        @Override public void open(Configuration configuration) throws Exception {
            socket = new Socket(host, port);
            output = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
            ObjectNode hello = JSON.createObjectNode();
            hello.put("type", "worker_ready");
            hello.put("route", getRuntimeContext().getIndexOfThisSubtask());
            hello.put("host", "127.0.0.1"); hello.put("jvm", "local-test");
            output.println(hello.toString());
        }
        @Override public void invoke(Tuple2<Integer, String> value, Context context) throws Exception {
            if (value.f0 != getRuntimeContext().getIndexOfThisSubtask()) throw new AssertionError("Wrong downstream route");
            ObjectNode result = (ObjectNode) JSON.readTree(value.f1);
            result.put("type", "result");
            output.println(result.toString());
            if (output.checkError()) throw new IOException("Echo result connection failed");
        }
        @Override public void close() throws Exception { if (socket != null) socket.close(); }
    }

    private static void unitChecks() throws Exception {
        JobManagerGlobalIndexSource.Request request = new JobManagerGlobalIndexSource.Request("{\"query_id\":\"q\",\"route\":2,\"box\":[0,1,0,1],\"entries\":[10]}");
        JobManagerGlobalIndexSource.RequestSerializer splits = new JobManagerGlobalIndexSource.RequestSerializer();
        JobManagerGlobalIndexSource.Request restored = splits.deserialize(splits.getVersion(), splits.serialize(request));
        if (!request.json.equals(restored.json) || !request.splitId().equals(restored.splitId())) throw new AssertionError("Split round trip");
        try { splits.deserialize(999, splits.serialize(request)); throw new AssertionError("Invalid version accepted"); }
        catch (IOException expected) {}
        JobManagerGlobalIndexSource.StateSerializer states = new JobManagerGlobalIndexSource.StateSerializer();
        String state = "{\"ownership\":{\"10\":0},\"pending\":[],\"stopping\":false}";
        if (!state.equals(states.deserialize(states.getVersion(), states.serialize(state)))) throw new AssertionError("Enumerator state round trip");

        File file = File.createTempFile("mgsi-tree-check", ".json");
        try {
            ObjectNode metadata = JSON.createObjectNode();
            ArrayNode entries = metadata.putArray("entries");
            for (int id : new int[] {2, 10}) {
                ObjectNode entry = entries.addObject();
                entry.put("entry_id", id); entry.put("lon_min", 0); entry.put("lat_min", 0);
                entry.put("lon_max", 5); entry.put("lat_max", 5);
            }
            JSON.writeValue(file, metadata);
            JobManagerGlobalIndexSource.ChunkTree tree = new JobManagerGlobalIndexSource.ChunkTree(file.toString());
            if (tree.root.chunk.id != 10) throw new AssertionError("Equal-distance IDs must use lexical order");
            if (tree.query(new double[] {4, 5, 4, 5}).size() != 2) throw new AssertionError("Complete bounds, not representative-only membership");
            if (tree.query(new double[] {5, 5, 5, 5}).size() != 2) throw new AssertionError("Inclusive boundary");
            if (!tree.query(new double[] {6, 7, 6, 7}).isEmpty()) throw new AssertionError("Outside query");
            try { tree.query(new double[] {0, Double.NaN, 0, 1}); throw new AssertionError("NaN accepted"); }
            catch (IllegalArgumentException expected) {}
            // Identical representatives exercise the iterative skewed-tree path.
            entries.removeAll();
            for (int id = 0; id < 600; id++) {
                ObjectNode entry = entries.addObject();
                entry.put("entry_id", id); entry.put("lon_min", 0); entry.put("lat_min", 0);
                entry.put("lon_max", 1); entry.put("lat_max", 1);
            }
            JSON.writeValue(file, metadata);
            tree = new JobManagerGlobalIndexSource.ChunkTree(file.toString());
            if (tree.maximumDepth != 599 || tree.query(new double[] {0, 1, 0, 1}).size() != 600) throw new AssertionError("Skewed tree lost chunks");
        } finally { if (!file.delete()) file.deleteOnExit(); }
        System.out.println("MGSI_SOURCE_UNIT_PASS");
    }

    public static void main(String[] args) throws Exception {
        unitChecks();
        String host = args[0]; int port = Integer.parseInt(args[1]); String metadata = args[2];
        Configuration configuration = new Configuration();
        configuration.setString("jobmanager.bind-host", "127.0.0.1");
        configuration.setString("taskmanager.host", "127.0.0.1");
        configuration.setString("taskmanager.memory.network.min", "16m");
        configuration.setString("taskmanager.memory.network.max", "16m");
        configuration.setString("taskmanager.memory.managed.size", "32m");
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironment(2, configuration);
        environment.setBufferTimeout(0);
        environment.setRestartStrategy(RestartStrategies.noRestart());
        environment.fromSource(new JobManagerGlobalIndexSource(host, port, metadata, 2),
                WatermarkStrategy.<Tuple2<Integer, String>>noWatermarks(), "MGSI JobManager source integration",
                TypeInformation.of(new TypeHint<Tuple2<Integer, String>>() {})).setParallelism(1)
            .partitionCustom(new ResidentMGSI.DirectPartition(), 0)
            .addSink(new EchoSink(host, port)).setParallelism(2);
        environment.execute("MGSI JobManager global-index functional check");
        System.out.println("MGSI_SOURCE_INTEGRATION_PASS");
    }
}
