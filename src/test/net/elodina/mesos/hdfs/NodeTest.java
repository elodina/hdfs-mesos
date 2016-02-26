package net.elodina.mesos.hdfs;

import org.junit.Test;

import static org.apache.mesos.Protos.*;
import static org.junit.Assert.*;

public class NodeTest extends MesosTestCase {
    @Test
    public void matches() {
        Node node = new Node("0");
        node.cpus = 0.5;
        node.mem = 500;

        assertEquals("cpus < 0.5", node.matches(offer("cpus:0.1")));
        assertEquals("mem < 500", node.matches(offer("cpus:0.5; mem:400")));

        assertNull(node.matches(offer("cpus:0.5; mem:500; ports:0..4")));
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

        // complete reservation
        reservation = node.reserve(offer("cpus:0.7;mem:1000;ports:0..10"));
        assertEquals(node.cpus, reservation.cpus, 0.001);
        assertEquals(node.mem, reservation.mem);
    }

    @Test
    public void initRuntime() {
        Node node = new Node("0");
        Offer offer = offer("cpus:0.1;mem:100");
        node.initRuntime(offer);

        assertNotNull(node.runtime);
        assertNotNull(node.runtime.taskId);
        assertNotNull(node.runtime.executorId);
        assertEquals(offer.getSlaveId().getValue(), node.runtime.slaveId);

        assertNotNull(node.reservation);
        assertEquals(0.1, node.reservation.cpus, 0.001);
        assertEquals(100, node.reservation.mem);
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
        assertTrue(cmd, cmd.contains(Executor.class.getName()));
    }

    @Test
    public void toJson_fromJson() {
        Node node = new Node("node");
        node.type = Node.Type.NAME_NODE;
        node.state = Node.State.RUNNING;

        node.cpus = 2;
        node.mem = 1024;

        node.runtime = new Node.Runtime();
        node.reservation = new Node.Reservation();

        Node read = new Node(node.toJson());
        assertEquals(node.id, read.id);
        assertEquals(node.type, read.type);
        assertEquals(node.state, read.state);

        assertEquals(node.cpus, read.cpus, 0.001);
        assertEquals(node.mem, read.mem);

        assertNotNull(read.runtime);
        assertNotNull(read.reservation);
    }

    // Runtime
    @Test
    public void Runtime_toJson_fromJson() {
        Node.Runtime runtime = new Node.Runtime();
        runtime.killSent = true;

        Node.Runtime read = new Node.Runtime(runtime.toJson());
        assertEquals(runtime.taskId, read.taskId);
        assertEquals(runtime.executorId, read.executorId);
        assertEquals(runtime.slaveId, read.slaveId);

        assertEquals(runtime.killSent, read.killSent);
    }

    // Reservation
    @Test
    public void Reservation_toJson_fromJson() {
        Node.Reservation reservation = new Node.Reservation();
        reservation.cpus = 0.5;
        reservation.mem = 256;

        Node.Reservation read = new Node.Reservation(reservation.toJson());
        assertEquals(reservation.cpus, read.cpus, 0.001);
        assertEquals(reservation.mem, read.mem);
    }

    @Test
    public void Reservation_toResources() {
        assertEquals(resources(""), new Node.Reservation().toResources());
        assertEquals(resources("cpus:0.5;mem:500;"), new Node.Reservation(0.5, 500).toResources());
    }
}
