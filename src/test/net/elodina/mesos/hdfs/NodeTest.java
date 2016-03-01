package net.elodina.mesos.hdfs;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static net.elodina.mesos.hdfs.Util.Range;
import static org.apache.mesos.Protos.*;
import static org.junit.Assert.*;

public class NodeTest extends MesosTestCase {
    @Test
    public void matches() {
        Node node = new Node("0", Node.Type.NAME_NODE);
        node.cpus = 0.5;
        node.mem = 500;

        assertEquals("cpus < 0.5", node.matches(offer("cpus:0.1")));
        assertEquals("mem < 500", node.matches(offer("cpus:0.5; mem:400")));

        assertNull(node.matches(offer("cpus:0.5; mem:500; ports:0..4")));

        // no running name node
        node.type = Node.Type.DATA_NODE;
        assertEquals("no running name node", node.matches(offer("cpus:0.5; mem:500; ports:0..4")));
    }

    @Test
    public void reserve() {
        Node node = new Node("0");
        node.cpus = 0.5;
        node.mem = 400;

        // incomplete reservation
        Node.Reservation reservation = node.reserve(offer("cpus:0.3;mem:300"));
        assertEquals(0.3d, reservation.cpus, 0.001);
        assertEquals(300, reservation.mem);
        assertTrue("" + reservation.ports, reservation.ports.isEmpty());

        // complete reservation
        reservation = node.reserve(offer("cpus:0.7;mem:1000;ports:0..10"));
        assertEquals(node.cpus, reservation.cpus, 0.001);
        assertEquals(node.mem, reservation.mem);
        assertEquals(reservation.ports, Collections.singletonMap(Node.Port.NAME_NODE, 0));
    }

    @Test
    public void reservePort() {
        Node node = new Node("0");
        List<Range> ports = new ArrayList<>();
        ports.add(new Range("0..100"));

        assertEquals(10, node.reservePort(new Range("10..20"), ports));
        assertEquals(Arrays.asList(new Range("0..9"), new Range("11..100")), ports);

        assertEquals(0, node.reservePort(new Range("0..0"), ports));
        assertEquals(Arrays.asList(new Range("1..9"), new Range("11..100")), ports);

        assertEquals(100, node.reservePort(new Range("100..200"), ports));
        assertEquals(Arrays.asList(new Range("1..9"), new Range("11..99")), ports);

        assertEquals(50, node.reservePort(new Range("50..60"), ports));
        assertEquals(Arrays.asList(new Range("1..9"), new Range("11..49"), new Range("51..99")), ports);
    }

    @Test
    public void initRuntime() {
        Node node = new Node("0");
        Offer offer = offer("cpus:0.1;mem:100");
        node.initRuntime(offer);

        assertNotNull(node.runtime);
        assertNotNull(node.runtime.taskId);
        assertNotNull(node.runtime.executorId);
        assertNotNull(node.runtime.fsUri);
        assertEquals(offer.getSlaveId().getValue(), node.runtime.slaveId);

        assertNotNull(node.reservation);
        assertEquals(0.1, node.reservation.cpus, 0.001);
        assertEquals(100, node.reservation.mem);
    }

    @Test
    public void initRuntime_fsUri() {
        Node node = new Node("0", Node.Type.NAME_NODE);

        // name node
        node.initRuntime(offer());
        assertTrue(node.runtime.fsUri, node.runtime.fsUri.contains(offer().getHostname()));

        // data node, no name node
        node.type = Node.Type.DATA_NODE;
        try { node.initRuntime(offer()); fail(); }
        catch (IllegalStateException e) { assertTrue(e.getMessage(), e.getMessage().contains("name node host")); }

        // data node, running name node
        Node nn = Nodes.addNode(new Node("1", Node.Type.NAME_NODE));
        nn.initRuntime(offer());
        assertTrue(node.runtime.fsUri, node.runtime.fsUri.contains(nn.runtime.hostname));
    }

    @Test
    public void newTask() {
        Node node = new Node("0");
        node.initRuntime(offer());

        TaskInfo task = node.newTask();
        assertEquals("hdfs-" + node.id, task.getName());
        assertEquals(task.getTaskId().getValue(), node.runtime.taskId);
        assertEquals(task.getSlaveId().getValue(), node.runtime.slaveId);

        assertNotNull(task.getExecutor());
        assertEquals("" + node.toJson(), task.getData().toStringUtf8());
        assertEquals(node.reservation.toResources(), task.getResourcesList());
    }

    @Test
    public void newExecutor() {
        Node node = new Node("0");
        node.executorJvmOpts = "-Xmx100m";
        node.initRuntime(offer());

        ExecutorInfo executor = node.newExecutor();
        assertEquals("hdfs-" + node.id, executor.getName());
        assertEquals(node.runtime.executorId, executor.getExecutorId().getValue());

        // uris
        CommandInfo command = executor.getCommand();
        assertEquals(2, command.getUrisCount());

        String uri = command.getUris(0).getValue();
        assertTrue(uri, uri.contains(Scheduler.$.config.jar.getName()));
        uri = command.getUris(1).getValue();
        assertTrue(uri, uri.contains(Scheduler.$.config.hadoop.getName()));

        // cmd
        String cmd = command.getValue();
        assertTrue(cmd, cmd.contains("java"));
        assertTrue(cmd, cmd.contains(node.executorJvmOpts));
        assertTrue(cmd, cmd.contains(Executor.class.getName()));
    }

    @Test
    public void toJson_fromJson() {
        Node node = new Node("node");
        node.type = Node.Type.NAME_NODE;
        node.state = Node.State.RUNNING;

        node.cpus = 2;
        node.mem = 1024;
        node.executorJvmOpts = "executor-opts";
        node.executorJvmOpts = "hadoop-opts";

        node.initRuntime(offer());

        Node read = new Node(node.toJson());
        assertEquals(node.id, read.id);
        assertEquals(node.type, read.type);
        assertEquals(node.state, read.state);

        assertEquals(node.cpus, read.cpus, 0.001);
        assertEquals(node.mem, read.mem);
        assertEquals(node.executorJvmOpts, read.executorJvmOpts);
        assertEquals(node.hadoopJvmOpts, read.hadoopJvmOpts);

        assertNotNull(read.runtime);
        assertNotNull(read.reservation);
    }

    // Runtime
    @Test
    public void Runtime_toJson_fromJson() {
        Node node = new Node("0");
        Node.Runtime runtime = node.new Runtime();
        runtime.slaveId = "slaveId";
        runtime.hostname = "hostname";

        runtime.fsUri = "hdfs://localhost:31000";
        runtime.killSent = true;

        Node.Runtime read = node.new Runtime(runtime.toJson());
        assertEquals(runtime.taskId, read.taskId);
        assertEquals(runtime.executorId, read.executorId);

        assertEquals(runtime.slaveId, read.slaveId);
        assertEquals(runtime.hostname, read.hostname);

        assertEquals(runtime.fsUri, read.fsUri);
        assertEquals(runtime.killSent, read.killSent);
    }

    // Reservation
    @Test
    public void Reservation_toJson_fromJson() {
        Node.Reservation reservation = new Node.Reservation();
        reservation.cpus = 0.5;
        reservation.mem = 256;
        reservation.ports.put(Node.Port.NAME_NODE, 10);
        reservation.ports.put(Node.Port.DATA_NODE, 20);

        Node.Reservation read = new Node.Reservation(reservation.toJson());
        assertEquals(reservation.cpus, read.cpus, 0.001);
        assertEquals(reservation.mem, read.mem);
        assertEquals(reservation.ports, read.ports);
    }

    @Test
    public void Reservation_toResources() {
        assertEquals(resources(""), new Node.Reservation().toResources());
        assertEquals(resources("cpus:0.5;mem:500;ports:1000..1000"), new Node.Reservation(0.5, 500, Collections.singletonMap("namenode", 1000)).toResources());
    }
}
