package net.elodina.mesos.hdfs;

import com.google.protobuf.ByteString;
import org.apache.mesos.Protos;
import org.json.simple.JSONObject;

import java.util.*;

import static net.elodina.mesos.hdfs.Util.Period;
import static net.elodina.mesos.hdfs.Util.Range;
import static org.apache.mesos.Protos.*;

public class Node {
    public String id;
    public Type type = Type.NAMENODE;
    public State state = State.IDLE;

    public double cpus = 0.5;
    public long mem = 512;
    public String executorJvmOpts;
    public String hadoopJvmOpts;

    public Runtime runtime;
    public Reservation reservation;

    public Node() {}
    public Node(String id) { this.id = id; }
    public Node(String id, Node.Type type) { this.id = id; this.type = type; }
    public Node(JSONObject json) { fromJson(json); }

    public String matches(Offer offer) {
        Reservation reservation = reserve(offer);

        if (reservation.cpus < cpus) return "cpus < " + cpus;
        if (reservation.mem < mem) return "mem < " + mem;

        if (type == Type.DATANODE) {
            List<Node> nns = Nodes.getNodes(Node.Type.NAMENODE);
            boolean nnRunning = !nns.isEmpty() && nns.get(0).state == Node.State.RUNNING;
            if (!nnRunning) return "no running name node";
        }

        return null;
    }

    public Reservation reserve(Offer offer) {
        Map<String, Resource> resources = new HashMap<>();
        for (Resource resource : offer.getResourcesList()) resources.put(resource.getName(), resource);

        // cpu
        double reservedCpus = 0;
        Resource cpusResource = resources.get("cpus");
        if (cpusResource != null) reservedCpus = Math.min(cpusResource.getScalar().getValue(), cpus);

        // mem
        long reservedMem = 0;
        Resource memResource = resources.get("mem");
        if (memResource != null) reservedMem = Math.min((long) memResource.getScalar().getValue(), mem);

        // ports
        Map<String, Integer> reservedPorts = reservePorts(offer);

        return new Reservation(reservedCpus, reservedMem, reservedPorts);
    }

    private Map<String, Integer> reservePorts(Offer offer) {
        Map<String, Integer> ports = new HashMap<>();

        // find resource
        Resource portsResource = null;
        for (Resource resource : offer.getResourcesList())
            if (resource.getName().equals("ports")) { portsResource = resource; break; }
        if (portsResource == null) return ports;

        // collect & sort ranges
        List<Range> availPorts = new ArrayList<>();
        for (Value.Range r : portsResource.getRanges().getRangeList())
            availPorts.add(new Range((int) r.getBegin(), (int) r.getEnd()));

        Collections.sort(availPorts, new Comparator<Range>() {
            public int compare(Range x, Range y) { return x.start() - y.start(); }
        });

        // reserve ports
        for (String name : Node.Port.names(type)) {
            int port = reservePort(null, availPorts);
            if (port != -1) ports.put(name, port);
        }

        return ports;
    }

    int reservePort(Range range, List<Range> availPorts) {
        Range r = null;
        if (range == null)
            r = !availPorts.isEmpty() ? availPorts.get(0) : null; // take first avail range
        else
            for (Range t : availPorts) // take first range overlapping with ports
                if (range.overlap(t) != null) { r = t; break; }

        if (r == null) return -1;
        int port = range != null ? r.overlap(range).start() : r.start();

        // remove allocated port
        int idx = availPorts.indexOf(r);
        availPorts.remove(r);
        availPorts.addAll(idx, r.split(port));

        return port;
    }

    public boolean waitFor(State state, Period timeout) throws InterruptedException {
        long t = timeout.ms();

        while (t > 0 && this.state != state) {
            long delay = Math.min(100, t);
            Thread.sleep(delay);
            t -= delay;
        }

        return this.state == state;
    }

    public void initRuntime(Offer offer) {
        reservation = reserve(offer);

        runtime = new Runtime();
        runtime.slaveId = offer.getSlaveId().getValue();
        runtime.hostname = offer.getHostname();

        runtime.fsUri = getFsUri();
    }

    private String getFsUri() {
        List<Node> nodes = Nodes.getNodes(Type.NAMENODE);
        Node node = !nodes.isEmpty() ? nodes.get(0) : null;

        if (node == null) throw new IllegalStateException("no namenode");
        if (node.runtime == null) throw new IllegalStateException("namenode not started");

        String host = node.runtime.hostname;
        Integer port = node.reservation.ports.get(Port.IPC);
        if (port == null) throw new IllegalStateException("no ipc port");

        return "hdfs://" + host + ":" + port;
    }

    public TaskInfo newTask() {
        if (runtime == null) throw new IllegalStateException("runtime == null");
        if (reservation == null) throw new IllegalStateException("reservation == null");

        return TaskInfo.newBuilder()
            .setName("hdfs-" + id)
            .setTaskId(TaskID.newBuilder().setValue(runtime.taskId))
            .setSlaveId(SlaveID.newBuilder().setValue(runtime.slaveId))
            .setExecutor(newExecutor())
            .setData(ByteString.copyFromUtf8("" + toJson()))
            .addAllResources(reservation.toResources())
            .build();
    }

    ExecutorInfo newExecutor() {
        if (runtime == null) throw new IllegalStateException("runtime == null");
        CommandInfo.Builder commandBuilder = CommandInfo.newBuilder();

        Scheduler.Config config = Scheduler.$.config;
        String cmd = "java -cp " + config.jar.getName();
        if (executorJvmOpts != null) cmd += " " + executorJvmOpts;
        cmd += " net.elodina.mesos.hdfs.Executor";

        commandBuilder
            .addUris(CommandInfo.URI.newBuilder().setValue(config.api + "/jar/" + config.jar.getName()).setExtract(false))
            .addUris(CommandInfo.URI.newBuilder().setValue(config.api + "/hadoop/" + config.hadoop.getName()))
            .setValue(cmd);

        return ExecutorInfo.newBuilder()
            .setName("hdfs-" + id)
            .setExecutorId(ExecutorID.newBuilder().setValue(runtime.executorId))
            .setCommand(commandBuilder)
            .build();
    }

    @SuppressWarnings("unchecked")
    public JSONObject toJson() {
        JSONObject json = new JSONObject();

        json.put("id", id);
        json.put("type", type.name().toLowerCase());
        json.put("state", "" + state.name().toLowerCase());

        json.put("cpus", cpus);
        json.put("mem", mem);
        if (executorJvmOpts != null) json.put("executorJvmOpts", executorJvmOpts);
        if (hadoopJvmOpts != null) json.put("hadoopJvmOpts", hadoopJvmOpts);

        if (runtime != null) json.put("runtime", runtime.toJson());
        if (reservation != null) json.put("reservation", reservation.toJson());

        return json;
    }

    public void fromJson(JSONObject json) {
        id = (String) json.get("id");
        type = Type.valueOf(json.get("type").toString().toUpperCase());
        state = State.valueOf(json.get("state").toString().toUpperCase());

        cpus = ((Number) json.get("cpus")).doubleValue();
        mem = ((Number) json.get("mem")).longValue();
        if (json.containsKey("executorJvmOpts")) executorJvmOpts = (String) json.get("executorJvmOpts");
        if (json.containsKey("hadoopJvmOpts")) hadoopJvmOpts = (String) json.get("hadoopJvmOpts");

        if (json.containsKey("runtime")) runtime = new Runtime((JSONObject) json.get("runtime"));
        if (json.containsKey("reservation")) reservation = new Reservation((JSONObject) json.get("reservation"));
    }

    public int hashCode() { return id.hashCode(); }

    public boolean equals(Object obj) { return obj instanceof Node && ((Node) obj).id.equals(id); }

    public String toString() { return id; }

    public enum State {
        IDLE,
        STARTING,
        RUNNING,
        STOPPING,
        RECONCILING
    }

    public enum Type {
        NAMENODE,
        DATANODE
    }

    public static class Port {
        public static final String HTTP = "http";
        public static final String IPC = "ipc";
        public static final String DATA = "data";

        public static String[] names(Type type) {
            return type == Type.NAMENODE ?
                new String[]{HTTP, IPC} :
                new String[]{ HTTP, IPC, DATA };
        }
    }

    public static class Runtime {
        public String taskId = "" + UUID.randomUUID();
        public String executorId = "" + UUID.randomUUID();

        public String slaveId;
        public String hostname;

        public String fsUri;
        public boolean killSent;

        public Runtime() {}
        public Runtime(JSONObject json) { fromJson(json); }

        @SuppressWarnings("unchecked")
        public JSONObject toJson() {
            JSONObject json = new JSONObject();

            json.put("taskId", taskId);
            json.put("executorId", executorId);

            json.put("slaveId", slaveId);
            json.put("hostname", hostname);

            json.put("fsUri", fsUri);
            json.put("killSent", killSent);

            return json;
        }

        public void fromJson(JSONObject json) {
            taskId = (String) json.get("taskId");
            executorId = (String) json.get("executorId");

            slaveId = (String) json.get("slaveId");
            hostname = (String) json.get("hostname");

            fsUri = (String) json.get("fsUri");
            killSent = (boolean) json.get("killSent");
        }
    }

    public static class Reservation {
        double cpus = 0;
        long mem = 0;
        Map<String, Integer> ports = new HashMap<>();

        public Reservation() {}

        public Reservation(double cpus, long mem, Map<String, Integer> ports) {
            this.cpus = cpus;
            this.mem = mem;
            this.ports = ports;
        }

        public Reservation(JSONObject json) { fromJson(json); }

        public List<Resource> toResources() {
            class R {
                Resource cpus(double value) {
                    return Resource.newBuilder()
                        .setName("cpus")
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder().setValue(value))
                        .setRole("*")
                        .build();
                }

                Resource mem(long value) {
                    return Resource.newBuilder()
                        .setName("mem")
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder().setValue(value))
                        .setRole("*")
                        .build();
                }

                Resource port(long value) {
                    return Resource.newBuilder()
                        .setName("ports")
                        .setType(Protos.Value.Type.RANGES)
                        .setRanges(Protos.Value.Ranges.newBuilder().addRange(Protos.Value.Range.newBuilder().setBegin(value).setEnd(value)))
                        .setRole("*")
                        .build();
                }
            }

            R r = new R();

            List<Resource> resources = new ArrayList<>();

            if (cpus > 0) resources.add(r.cpus(cpus));
            if (mem > 0) resources.add(r.mem(mem));

            for (String name : ports.keySet())
                resources.add(r.port(ports.get(name)));

            return resources;
        }

        public void fromJson(JSONObject json) {
            cpus = (double) json.get("cpus");
            mem = (long) json.get("mem");

            ports.clear();
            if (json.containsKey("ports")) {
                JSONObject portsJson = (JSONObject) json.get("ports");
                for (Object name : portsJson.keySet())
                    ports.put("" + name, ((Number) portsJson.get(name)).intValue());
            }
        }

        @SuppressWarnings("unchecked")
        public JSONObject toJson() {
            JSONObject json = new JSONObject();

            json.put("cpus", cpus);
            json.put("mem", mem);

            if (!ports.isEmpty()) json.put("ports", new JSONObject(ports));

            return json;
        }
    }
}
