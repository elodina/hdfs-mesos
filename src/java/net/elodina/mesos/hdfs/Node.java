package net.elodina.mesos.hdfs;

import com.google.protobuf.ByteString;
import org.apache.mesos.Protos;
import org.json.simple.JSONObject;

import java.util.*;

import static net.elodina.mesos.hdfs.Util.Period;
import static org.apache.mesos.Protos.*;

public class Node {
    public String id;
    public Type type = Type.DATA_NODE;
    public State state = State.IDLE;

    public double cpus = 0.5;
    public long mem = 512;

    public Runtime runtime;
    public Reservation reservation;

    public Node() {}
    public Node(String id) { this(id, Type.DATA_NODE); }
    public Node(String id, Node.Type type) { this.id = id; this.type = type; }
    public Node(JSONObject json) { fromJson(json); }

    public String matches(Offer offer) {
        Reservation reservation = reserve(offer);

        if (reservation.cpus < cpus) return "cpus < " + cpus;
        if (reservation.mem < mem) return "mem < " + mem;

        if (type == Type.DATA_NODE) {
            List<Node> nns = Nodes.getNodes(Node.Type.NAME_NODE);
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

        return new Reservation(reservedCpus, reservedMem);
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
        runtime.fsUri = "hdfs://" + offer.getHostname() + ":54310";
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
        NAME_NODE,
        DATA_NODE
    }

    public static class Runtime {
        public String taskId = "" + UUID.randomUUID();
        public String executorId = "" + UUID.randomUUID();
        public String slaveId = "" + UUID.randomUUID();

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

            json.put("fsUri", fsUri);
            json.put("killSent", killSent);

            return json;
        }

        public void fromJson(JSONObject json) {
            taskId = (String) json.get("taskId");
            executorId = (String) json.get("executorId");
            slaveId = (String) json.get("slaveId");

            fsUri = (String) json.get("fsUri");
            killSent = (boolean) json.get("killSent");
        }
    }

    public static class Reservation {
        double cpus = 0;
        long mem = 0;

        public Reservation() {}

        public Reservation(double cpus, long mem) {
            this.cpus = cpus;
            this.mem = mem;
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

            return resources;
        }

        public void fromJson(JSONObject json) {
            cpus = (double) json.get("cpus");
            mem = (long) json.get("mem");
        }

        @SuppressWarnings("unchecked")
        public JSONObject toJson() {
            JSONObject json = new JSONObject();

            json.put("cpus", cpus);
            json.put("mem", mem);

            return json;
        }
    }
}
