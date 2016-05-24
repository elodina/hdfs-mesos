package net.elodina.mesos.hdfs;

import net.elodina.mesos.api.*;
import net.elodina.mesos.util.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Node {
    public String id;
    public Type type = Type.NAMENODE;
    public State state = State.IDLE;

    public double cpus = 0.5;
    public long mem = 512;

    public Map<String, Constraint> constraints = new LinkedHashMap<>();

    public String executorJvmOpts;
    public String hadoopJvmOpts;
    public Map<String, String> coreSiteOpts = new HashMap<>();
    public Map<String, String> hdfsSiteOpts = new HashMap<>();

    public String externalFsUri;

    public Stickiness stickiness = new Stickiness();
    public Runtime runtime;
    public Reservation reservation;

    public Node() {}
    public Node(String id) { this.id = id; }
    public Node(String id, Node.Type type) { this.id = id; this.type = type; }
    public Node(JSONObject json) { fromJson(json); }

    public boolean isExternal() { return externalFsUri != null; }

    public String matches(Offer offer) { return matches(offer, Collections.<String, Collection<String>>emptyMap(), new Date()); }

    public String matches(Offer offer, Map<String, Collection<String>> otherAttributes) { return matches(offer, otherAttributes, new Date()); }

    public String matches(Offer offer, Date now) { return matches(offer, Collections.<String, Collection<String>>emptyMap(), now); }

    public String matches(Offer offer, Map<String, Collection<String>> otherAttributes, Date now) {
        Reservation reservation = reserve(offer);

        // resources
        if (reservation.cpus < cpus) return "cpus < " + cpus;
        if (reservation.mem < mem) return "mem < " + mem;

        // namenode running
        if (type == Type.DATANODE) {
            List<Node> nns = Nodes.getNodes(Node.Type.NAMENODE);
            Node nn = nns.isEmpty() ? null : nns.get(0);

            if (nn == null) return "no namenode";
            if (!nn.isExternal() && nn.state != State.RUNNING) return "no running or external namenode";
        }

        // constraints
        Map<String, String> offerAttributes = new HashMap<>();
        offerAttributes.put("hostname", offer.hostname());

        for (Attribute attribute : offer.attributes())
            offerAttributes.put(attribute.name(), "" + attribute.value());

        for (String name : constraints.keySet()) {
            Constraint constraint = constraints.get(name);
            if (!offerAttributes.containsKey(name)) return "no " + name + " attribute";
            if (!constraint.matches(offerAttributes.get(name), otherAttributes.get(name))) return name + " doesn't match " + constraint;
        }

        // stickiness
        if (!stickiness.allowsHostname(offer.hostname(), now))
            return "hostname != stickiness hostname";

        return null;
    }

    public Reservation reserve(Offer offer) {
        Map<String, Resource> resources = new HashMap<>();
        for (Resource resource : offer.resources()) resources.put(resource.name(), resource);

        // cpu
        double reservedCpus = 0;
        Resource cpusResource = resources.get("cpus");
        if (cpusResource != null) reservedCpus = Math.min(cpusResource.value().asDouble(), cpus);

        // mem
        long reservedMem = 0;
        Resource memResource = resources.get("mem");
        if (memResource != null) reservedMem = Math.min((long) memResource.value().asLong(), mem);

        // ports
        Map<String, Integer> reservedPorts = reservePorts(offer);

        return new Reservation(reservedCpus, reservedMem, reservedPorts);
    }

    private Map<String, Integer> reservePorts(Offer offer) {
        Map<String, Integer> ports = new HashMap<>();

        // find resource
        Resource portsResource = null;
        for (Resource resource : offer.resources())
            if (resource.name().equals("ports")) { portsResource = resource; break; }
        if (portsResource == null) return ports;

        // collect & sort ranges
        List<Range> availPorts = new ArrayList<>(portsResource.value().asRanges());
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
        runtime.slaveId = offer.slaveId();
        runtime.hostname = offer.hostname();

        for (Attribute attribute : offer.attributes())
            runtime.attributes.put(attribute.name(), "" + attribute.value());

        runtime.fsUri = getFsUri();
    }

    private String getFsUri() {
        List<Node> nodes = Nodes.getNodes(Type.NAMENODE);
        Node node = !nodes.isEmpty() ? nodes.get(0) : null;
        if (node == null) throw new IllegalStateException("no namenode");

        if (node.isExternal()) return node.externalFsUri;
        if (node.runtime == null) throw new IllegalStateException("namenode not started");

        String host = node.runtime.hostname;
        Integer port = node.reservation.ports.get(Port.IPC);
        if (port == null) throw new IllegalStateException("no ipc port");

        return "hdfs://" + host + ":" + port;
    }

    public void registerStart(String hostname) {
        stickiness.registerStart(hostname);
    }

    public void registerStop() { registerStop(new Date(), false); }
    public void registerStop(Date now, boolean failed) {
        if (!failed) stickiness.registerStop(now);
    }

    public Task newTask() {
        if (runtime == null) throw new IllegalStateException("runtime == null");
        if (reservation == null) throw new IllegalStateException("reservation == null");

        String data = "" + toJson();
        if (Scheduler.$.config.driverV1()) data = Base64.encode(data);

        return new Task()
            .id(runtime.taskId)
            .name("hdfs-" + id)
            .slaveId(runtime.slaveId)
            .executor(newExecutor())
            .data(data.getBytes())
            .resources(reservation.toResources());
    }

    Task.Executor newExecutor() {
        if (runtime == null) throw new IllegalStateException("runtime == null");

        Scheduler.Config config = Scheduler.$.config;
        String cmd = "java -cp " + config.jar.getName();
        if (executorJvmOpts != null) cmd += " " + executorJvmOpts;

        cmd += " net.elodina.mesos.hdfs.Executor";
        cmd += " --driver=" + config.driver;
        cmd += " --debug=" + config.debug;

        Command command = new Command()
            .addUri(new Command.URI(config.api + "/jar/" + config.jar.getName(), false).cache(false))
            .addUri(new Command.URI(config.api + "/hadoop/" + config.hadoop.getName()));

        if (config.jre != null) {
            command.addUri(new Command.URI(config.api + "/jre/" + config.jre.getName()));
            cmd = "jre/bin/" + cmd;
        }

        command.value(cmd);

        return new Task.Executor()
            .id(runtime.executorId)
            .name("hdfs-" + id)
            .command(command);
    }

    @SuppressWarnings("unchecked")
    public JSONObject toJson() {
        JSONObject json = new JSONObject();

        json.put("id", id);
        json.put("type", type.name().toLowerCase());
        json.put("state", "" + state.name().toLowerCase());

        json.put("cpus", cpus);
        json.put("mem", mem);

        if (!constraints.isEmpty()) json.put("constraints", Strings.formatMap(constraints));

        if (executorJvmOpts != null) json.put("executorJvmOpts", executorJvmOpts);
        if (hadoopJvmOpts != null) json.put("hadoopJvmOpts", hadoopJvmOpts);

        if (!coreSiteOpts.isEmpty()) json.put("coreSiteOpts", new JSONObject(coreSiteOpts));
        if (!hdfsSiteOpts.isEmpty()) json.put("hdfsSiteOpts", new JSONObject(hdfsSiteOpts));

        if (externalFsUri != null) json.put("externalFsUri", externalFsUri);

        json.put("stickiness", stickiness.toJson());
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

        constraints.clear();
        if (json.containsKey("constraints")) {
            Map<String, String> m = Strings.parseMap((String) json.get("constraints"));
            for (String name : m.keySet()) constraints.put(name, new Constraint(m.get(name)));
        }

        if (json.containsKey("executorJvmOpts")) executorJvmOpts = (String) json.get("executorJvmOpts");
        if (json.containsKey("hadoopJvmOpts")) hadoopJvmOpts = (String) json.get("hadoopJvmOpts");

        coreSiteOpts.clear();
        if (json.containsKey("coreSiteOpts")) {
            JSONObject coreSiteOptsJson = (JSONObject) json.get("coreSiteOpts");
            for (Object name : coreSiteOptsJson.keySet()) coreSiteOpts.put("" + name, "" + coreSiteOptsJson.get("" + name));
        }

        hdfsSiteOpts.clear();
        if (json.containsKey("hdfsSiteOpts")) {
            JSONObject hdfsSiteOptsJson = (JSONObject) json.get("hdfsSiteOpts");
            for (Object name : hdfsSiteOptsJson.keySet()) hdfsSiteOpts.put("" + name, "" + hdfsSiteOptsJson.get("" + name));
        }

        if (json.containsKey("externalFsUri")) externalFsUri = (String) json.get("externalFsUri");

        stickiness = new Stickiness((JSONObject) json.get("stickiness"));
        if (json.containsKey("runtime")) runtime = new Runtime((JSONObject) json.get("runtime"));
        if (json.containsKey("reservation")) reservation = new Reservation((JSONObject) json.get("reservation"));
    }

    @SuppressWarnings({"unchecked", "RedundantCast"})
    public static List<Node> fromJsonArray(JSONArray nodesJson) {
        List<Node> nodes = new ArrayList<>();
        for (JSONObject nodeJson : (List<JSONObject>) nodesJson) nodes.add(new Node(nodeJson));
        return nodes;
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
                new String[]{HTTP, IPC, DATA};
        }
    }

    public static class Runtime {
        public String taskId = "" + UUID.randomUUID();
        public String executorId = "" + UUID.randomUUID();

        public String slaveId;
        public String hostname;
        public Map<String, String> attributes = new LinkedHashMap<>();

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
            if (!attributes.isEmpty()) json.put("attributes", Strings.formatMap(attributes));

            json.put("fsUri", fsUri);
            json.put("killSent", killSent);

            return json;
        }

        public void fromJson(JSONObject json) {
            taskId = (String) json.get("taskId");
            executorId = (String) json.get("executorId");

            slaveId = (String) json.get("slaveId");
            hostname = (String) json.get("hostname");
            attributes.clear();
            if (json.containsKey("attributes")) attributes.putAll(Strings.parseMap((String) json.get("attributes")));

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
                    return new Resource("cpus", new Value(Value.Type.SCALAR, value));
                }

                Resource mem(long value) {
                    return new Resource("mem", new Value(Value.Type.SCALAR, (double)value));
                }

                Resource port(long value) {
                    return new Resource("ports", new Value(Value.Type.RANGES, Arrays.asList(new Range((int)value))));
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

    public static class Stickiness {
        private Period period = new Period("30m");
        private volatile String hostname;
        private volatile Date stopTime;

        public Stickiness() {}
        public Stickiness(JSONObject json) { fromJson(json); }

        public Period period() { return period; }
        public String hostname() { return hostname; }
        public Date stopTime() { return stopTime; }

        public Date expires() { return stopTime != null ? new Date(stopTime.getTime() + period.ms()) : null; }

        public void registerStart(String hostname) {
            this.hostname = hostname;
            stopTime = null;
        }

        public void registerStop() { registerStop(new Date()); }
        public void registerStop(Date now) {
            this.stopTime = now;
        }

        public boolean allowsHostname(String hostname) { return allowsHostname(hostname, new Date()); }

        @SuppressWarnings("SimplifiableIfStatement")
        public boolean allowsHostname(String hostname, Date now) {
            if (this.hostname == null) return true;
            if (stopTime == null || now.getTime() - stopTime.getTime() >= period.ms()) return true;
            return this.hostname.equals(hostname);
        }

        public void fromJson(JSONObject json) {
            period = new Period((String) json.get("period"));

            try { if (json.containsKey("stopTime")) stopTime = dateTimeFormat().parse((String) json.get("stopTime")); }
            catch (ParseException e) { throw new IllegalStateException(e); }

            if (json.containsKey("hostname")) hostname = (String) json.get("hostname");
        }

        @SuppressWarnings("unchecked")
        public JSONObject toJson() {
            JSONObject json = new JSONObject();

            json.put("period", "" + period);
            if (stopTime != null) json.put("stopTime", dateTimeFormat().format(stopTime));
            if (hostname != null) json.put("hostname", hostname);

            return json;
        }

        private static SimpleDateFormat dateTimeFormat() {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            format.setTimeZone(TimeZone.getTimeZone("UTC-0"));
            return format;
        }

    }
}
