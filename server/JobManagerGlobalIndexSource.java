import java.io.*;
import java.lang.management.ManagementFactory;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonToken;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.*;

/**
 * Global extent index in Flink's JobManager SourceCoordinator (Flink 1.11.3).
 * Only createEnumerator/restoreEnumerator construct the tree. Source readers
 * emit already routed requests and never build or query the global index.
 * The control socket carries query boxes, ownership updates and diagnostics.
 */
public final class JobManagerGlobalIndexSource
        implements Source<Tuple2<Integer, String>, JobManagerGlobalIndexSource.Request, String> {
    private static final long serialVersionUID = 1L;
    private static final ObjectMapper JSON = new ObjectMapper();
    private final String host, metadata;
    private final int port, routes;

    public JobManagerGlobalIndexSource(String host, int port, String metadata, int routes) {
        if (routes < 1) throw new IllegalArgumentException("Positive route count required");
        this.host = host;
        this.port = port;
        this.metadata = metadata;
        this.routes = routes;
    }

    @Override public Boundedness getBoundedness() { return Boundedness.CONTINUOUS_UNBOUNDED; }
    @Override public SourceReader<Tuple2<Integer, String>, Request> createReader(SourceReaderContext context) {
        return new RequestsReader();
    }
    @Override public SplitEnumerator<Request, String> createEnumerator(SplitEnumeratorContext<Request> context) throws Exception {
        return new IndexEnumerator(context, host, port, metadata, routes, null);
    }
    @Override public SplitEnumerator<Request, String> restoreEnumerator(SplitEnumeratorContext<Request> context, String state) throws Exception {
        return new IndexEnumerator(context, host, port, metadata, routes, state);
    }
    @Override public SimpleVersionedSerializer<Request> getSplitSerializer() { return new RequestSerializer(); }
    @Override public SimpleVersionedSerializer<String> getEnumeratorCheckpointSerializer() { return new StateSerializer(); }

    public static final class Request implements SourceSplit {
        final String json;
        final String id;
        final int route;
        Request(String json) throws IOException {
            this.json = json;
            JsonNode value = JSON.readTree(json);
            route = value.get("route").asInt();
            id = value.get("query_id").asText() + ":" + route;
        }
        @Override public String splitId() { return id; }
    }

    static final class RequestSerializer implements SimpleVersionedSerializer<Request> {
        @Override public int getVersion() { return 1; }
        @Override public byte[] serialize(Request split) { return split.json.getBytes(StandardCharsets.UTF_8); }
        @Override public Request deserialize(int version, byte[] bytes) throws IOException {
            requireVersion(version);
            return new Request(new String(bytes, StandardCharsets.UTF_8));
        }
    }
    static final class StateSerializer implements SimpleVersionedSerializer<String> {
        @Override public int getVersion() { return 1; }
        @Override public byte[] serialize(String state) { return state.getBytes(StandardCharsets.UTF_8); }
        @Override public String deserialize(int version, byte[] bytes) throws IOException {
            requireVersion(version);
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }
    private static void requireVersion(int version) throws IOException {
        if (version != 1) throw new IOException("Unsupported MGSI source state version " + version);
    }

    /** All methods run on the source task's mailbox thread; pollNext never blocks. */
    static final class RequestsReader implements SourceReader<Tuple2<Integer, String>, Request> {
        private final Deque<Request> queued = new ArrayDeque<>();
        private CompletableFuture<Void> available = new CompletableFuture<>();
        private boolean finished;
        @Override public void start() {}
        @Override public InputStatus pollNext(ReaderOutput<Tuple2<Integer, String>> output) {
            Request request = queued.pollFirst();
            if (request != null) output.collect(Tuple2.of(request.route, request.json));
            if (!queued.isEmpty()) return InputStatus.MORE_AVAILABLE;
            if (finished) return InputStatus.END_OF_INPUT;
            if (available.isDone()) available = new CompletableFuture<>();
            return InputStatus.NOTHING_AVAILABLE;
        }
        @Override public List<Request> snapshotState(long checkpointId) { return new ArrayList<>(queued); }
        @Override public CompletableFuture<Void> isAvailable() {
            return finished || !queued.isEmpty() ? CompletableFuture.completedFuture(null) : available;
        }
        @Override public void addSplits(List<Request> splits) {
            queued.addAll(splits);
            available.complete(null);
        }
        @Override public void notifyNoMoreSplits() { finished = true; available.complete(null); }
        @Override public void close() { finished = true; queued.clear(); available.complete(null); }
    }

    static final class Chunk {
        final int id;
        final String key;
        final double[] low, high;
        Chunk(JsonNode entry) {
            id = entry.get("entry_id").asInt();
            key = Integer.toString(id);
            low = new double[] {entry.get("lon_min").asDouble(), entry.get("lat_min").asDouble()};
            high = new double[] {entry.get("lon_max").asDouble(), entry.get("lat_max").asDouble()};
            validateBox(low, high);
        }
    }
    static void validateBox(double[] low, double[] high) {
        for (int dimension = 0; dimension < 2; dimension++) {
            if (!Double.isFinite(low[dimension]) || !Double.isFinite(high[dimension]) || low[dimension] > high[dimension])
                throw new IllegalArgumentException("Finite, non-wrapping closed bounds required");
        }
    }
    static boolean overlaps(double[] low, double[] high, double[] queryLow, double[] queryHigh) {
        return !(high[0] < queryLow[0] || low[0] > queryHigh[0] || high[1] < queryLow[1] || low[1] > queryHigh[1]);
    }

    /** Iterative centroid-pivot tree, lexical ID ties, complete-subtree bounds. */
    static final class ChunkTree {
        static final class Node {
            final Chunk chunk;
            final double[] low, high;
            Node left, right;
            Node(Chunk chunk, double[] low, double[] high) { this.chunk = chunk; this.low = low; this.high = high; }
        }
        static final class Build {
            final List<Chunk> group;
            final int depth;
            final Node parent;
            final boolean left;
            Build(List<Chunk> group, int depth, Node parent, boolean left) {
                this.group = group; this.depth = depth; this.parent = parent; this.left = left;
            }
        }
        Node root;
        final Set<Integer> ids = new HashSet<>();
        int maximumDepth = -1;
        ChunkTree(String metadata) throws IOException {
            List<Chunk> chunks = new ArrayList<>();
            // Skip native file/physical-chunk payloads; the global tree needs only entry extents.
            try (JsonParser parser = JSON.getFactory().createParser(new File(metadata))) {
                if (parser.nextToken() != JsonToken.START_OBJECT) throw new IOException("Metadata must be an object");
                boolean found = false;
                while (parser.nextToken() != JsonToken.END_OBJECT) {
                    String field = parser.getCurrentName();
                    parser.nextToken();
                    if ("entries".equals(field)) {
                        if (found || parser.currentToken() != JsonToken.START_ARRAY) throw new IOException("Invalid entries array");
                        found = true;
                        while (parser.nextToken() != JsonToken.END_ARRAY) {
                            Chunk chunk = new Chunk(JSON.readTree(parser));
                            if (!ids.add(chunk.id)) throw new IOException("Duplicate entry ID " + chunk.id);
                            chunks.add(chunk);
                        }
                    } else parser.skipChildren();
                }
                if (!found) throw new IOException("Missing entries array");
            }
            chunks.sort(Comparator.comparing(chunk -> chunk.key));
            Deque<Build> stack = new ArrayDeque<>();
            if (!chunks.isEmpty()) stack.push(new Build(chunks, 0, null, false));
            while (!stack.isEmpty()) {
                Build work = stack.pop();
                double[] centroid = new double[2], correction = new double[2];
                double[] low = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
                double[] high = {Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY};
                for (Chunk chunk : work.group) {
                    for (int d = 0; d < 2; d++) {
                        double value = chunk.low[d] / work.group.size() - correction[d];
                        double next = centroid[d] + value;
                        correction[d] = (next - centroid[d]) - value;
                        centroid[d] = next;
                        low[d] = Math.min(low[d], chunk.low[d]);
                        high[d] = Math.max(high[d], chunk.high[d]);
                    }
                }
                Chunk pivot = null;
                double best = Double.POSITIVE_INFINITY;
                for (Chunk chunk : work.group) {
                    double x = chunk.low[0] - centroid[0], y = chunk.low[1] - centroid[1];
                    double distance = x * x + y * y;
                    if (pivot == null || distance < best || (distance == best && chunk.key.compareTo(pivot.key) < 0)) {
                        pivot = chunk; best = distance;
                    }
                }
                Node node = new Node(pivot, low, high);
                if (work.parent == null) root = node;
                else if (work.left) work.parent.left = node;
                else work.parent.right = node;
                maximumDepth = Math.max(maximumDepth, work.depth);
                List<Chunk> left = new ArrayList<>(), right = new ArrayList<>();
                int dimension = work.depth % 2;
                for (Chunk chunk : work.group) if (chunk != pivot)
                    (chunk.low[dimension] < pivot.low[dimension] ? left : right).add(chunk);
                if (!right.isEmpty()) stack.push(new Build(right, work.depth + 1, node, false));
                if (!left.isEmpty()) stack.push(new Build(left, work.depth + 1, node, true));
            }
        }
        List<Integer> query(double[] box) {
            if (box.length != 4) throw new IllegalArgumentException("Box order is lat_min,lat_max,lon_min,lon_max");
            double[] low = {box[2], box[0]}, high = {box[3], box[1]};
            validateBox(low, high);
            List<Integer> selected = new ArrayList<>();
            Deque<Node> stack = new ArrayDeque<>();
            if (root != null) stack.push(root);
            while (!stack.isEmpty()) {
                Node node = stack.pop();
                if (!overlaps(node.low, node.high, low, high)) continue;
                if (overlaps(node.chunk.low, node.chunk.high, low, high)) selected.add(node.chunk.id);
                if (node.right != null) stack.push(node.right);
                if (node.left != null) stack.push(node.left);
            }
            return selected;
        }
    }

    static final class Control implements Closeable {
        final Socket socket;
        final BufferedReader input;
        final PrintWriter output;
        Control(String host, int port) throws IOException {
            socket = new Socket();
            socket.connect(new InetSocketAddress(host, port), 20000);
            socket.setTcpNoDelay(true);
            input = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            output = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
        }
        void send(ObjectNode value) throws IOException {
            output.println(value.toString());
            if (output.checkError()) throw new IOException("MGSI coordinator control connection failed");
        }
        @Override public void close() throws IOException { socket.close(); }
    }

    static final class IndexEnumerator implements SplitEnumerator<Request, String> {
        final SplitEnumeratorContext<Request> context;
        final String host;
        final int port, routes;
        final ChunkTree tree;
        final double constructionSeconds;
        final Map<Integer, Integer> ownership = new HashMap<>();
        final Deque<Request> pending = new ArrayDeque<>();
        volatile boolean closed;
        boolean stopping;
        Control control;

        IndexEnumerator(SplitEnumeratorContext<Request> context, String host, int port, String metadata, int routes, String state) throws Exception {
            if (context.currentParallelism() != 1) throw new IllegalArgumentException("Exactly one query source reader required");
            this.context = context; this.host = host; this.port = port; this.routes = routes;
            long started = System.nanoTime();
            tree = new ChunkTree(metadata);
            constructionSeconds = (System.nanoTime() - started) / 1e9;
            if (state != null) {
                JsonNode restored = JSON.readTree(state);
                if (restored.get("ownership").size() != 0) setOwnership(restored.get("ownership"));
                for (JsonNode request : restored.get("pending")) pending.addLast(new Request(request.asText()));
                stopping = restored.path("stopping").asBoolean();
            }
        }
        @Override public void start() {
            context.callAsync(() -> new Control(host, port), (connection, error) -> {
                if (closed) { if (connection != null) try { connection.close(); } catch (IOException ignored) {} return; }
                if (error != null) throw new RuntimeException("Opening MGSI coordinator control", error);
                control = connection;
                ObjectNode hello = JSON.createObjectNode();
                hello.put("type", "source");
                hello.put("global_index_location", "Flink JobManager");
                hello.put("global_index_component", "SourceCoordinator/SplitEnumerator");
                hello.put("coordinator_jvm", ManagementFactory.getRuntimeMXBean().getName());
                hello.put("coordinator_thread", Thread.currentThread().getName());
                hello.put("construction_seconds", constructionSeconds);
                hello.put("chunks", tree.ids.size());
                hello.put("maximum_depth", tree.maximumDepth);
                try { control.send(hello); } catch (IOException exception) { throw new RuntimeException(exception); }
                dispatchPending();
                if (!stopping) readNext();
            });
        }
        private void readNext() {
            // Socket reads run in Flink's async executor. The handler, tree query
            // and split assignment run back on the JobManager coordinator thread.
            context.callAsync(() -> control.input.readLine(), (line, error) -> {
                if (closed) return;
                if (error != null) throw new RuntimeException("Reading MGSI coordinator command", error);
                if (line == null) throw new RuntimeException("MGSI driver disconnected without stop");
                try { handleCommand(JSON.readTree(line)); } catch (Exception exception) { throw new RuntimeException(exception); }
                if (!stopping) readNext();
            });
        }
        void handleCommand(JsonNode command) throws Exception {
            String type = command.path("type").asText();
            if ("configure_ownership".equals(type)) {
                setOwnership(command.get("assignment"));
                ObjectNode ack = JSON.createObjectNode();
                ack.put("type", "ownership_ready"); ack.put("entries", ownership.size());
                control.send(ack);
            } else if ("query".equals(type)) {
                String queryId = command.path("query_id").asText();
                if (queryId.isEmpty()) throw new IllegalArgumentException("Missing query_id");
                if (command.has("entries") || command.has("route")) throw new IllegalArgumentException("Driver must send a query box, not preselected entries/routes");
                JsonNode boxNode = command.get("box");
                if (boxNode == null || !boxNode.isArray() || boxNode.size() != 4) throw new IllegalArgumentException("Four query-box coordinates required");
                double[] box = new double[4];
                for (int i = 0; i < 4; i++) box[i] = boxNode.get(i).asDouble();
                long started = System.nanoTime();
                List<Integer> selected;
                if (command.path("initialization_only").asBoolean()) selected = Collections.emptyList();
                else {
                    if (ownership.size() != tree.ids.size()) throw new IllegalStateException("Ownership must be configured before data queries");
                    selected = tree.query(box);
                }
                List<ArrayNode> byRoute = new ArrayList<>();
                for (int route = 0; route < routes; route++) byRoute.add(JSON.createArrayNode());
                for (int id : selected) byRoute.get(ownership.get(id)).add(id);
                ObjectNode plan = JSON.createObjectNode();
                plan.put("type", "query_plan"); plan.put("query_id", queryId);
                plan.put("global_index_location", "Flink JobManager");
                plan.put("coordinator_jvm", ManagementFactory.getRuntimeMXBean().getName());
                plan.put("coordinator_thread", Thread.currentThread().getName());
                plan.put("global_query_seconds", (System.nanoTime() - started) / 1e9);
                ArrayNode ids = plan.putArray("selected_entries");
                for (int id : selected) ids.add(id);
                control.send(plan);
                for (int route = 0; route < routes; route++) {
                    ObjectNode request = JSON.createObjectNode();
                    request.put("type", "query"); request.put("route", route); request.put("query_id", queryId);
                    request.set("box", boxNode.deepCopy()); request.set("entries", byRoute.get(route));
                    if (command.has("clear_epoch")) request.set("clear_epoch", command.get("clear_epoch"));
                    pending.addLast(new Request(request.toString()));
                }
                dispatchPending();
            } else if ("stop".equals(type)) { stopping = true; dispatchPending(); }
            else throw new IllegalArgumentException("Unknown MGSI coordinator command " + type);
        }
        private void setOwnership(JsonNode assignment) {
            if (assignment == null || !assignment.isObject() || assignment.size() != tree.ids.size())
                throw new IllegalArgumentException("Ownership must contain every indexed entry exactly once");
            Map<Integer, Integer> validated = new HashMap<>();
            Iterator<Map.Entry<String, JsonNode>> fields = assignment.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                int id = Integer.parseInt(field.getKey()), route = field.getValue().asInt(-1);
                if (!tree.ids.contains(id) || route < 0 || route >= routes || validated.put(id, route) != null)
                    throw new IllegalArgumentException("Invalid ownership entry " + field.getKey());
            }
            ownership.clear(); ownership.putAll(validated);
        }
        private void dispatchPending() {
            if (!context.registeredReaders().containsKey(0)) return;
            if (!pending.isEmpty()) {
                context.assignSplits(new SplitsAssignment<>(Collections.singletonMap(0, new ArrayList<>(pending))));
                pending.clear();
            }
            if (stopping) context.signalNoMoreSplits(0);
        }
        @Override public void addReader(int reader) { if (reader != 0) throw new IllegalArgumentException("Unexpected reader"); dispatchPending(); }
        @Override public void handleSplitRequest(int reader, String hostname) { dispatchPending(); }
        @Override public void addSplitsBack(List<Request> splits, int reader) {
            for (int i = splits.size() - 1; i >= 0; i--) pending.addFirst(splits.get(i));
            dispatchPending();
        }
        @Override public String snapshotState() {
            ObjectNode state = JSON.createObjectNode();
            ObjectNode assignment = state.putObject("ownership");
            for (Map.Entry<Integer, Integer> entry : ownership.entrySet()) assignment.put(Integer.toString(entry.getKey()), entry.getValue());
            ArrayNode queued = state.putArray("pending");
            for (Request request : pending) queued.add(request.json);
            state.put("stopping", stopping);
            return state.toString();
        }
        @Override public void close() throws IOException { closed = true; if (control != null) control.close(); }
    }
}
