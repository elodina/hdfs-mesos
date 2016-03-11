package net.elodina.mesos.hdfs;

import org.apache.mesos.Protos;
import org.junit.Test;

import java.util.Arrays;

import static org.apache.mesos.Protos.TaskStatus;
import static org.junit.Assert.*;

public class SchedulerTest extends MesosTestCase {
    @Test
    public void onTaskStarted() {
        TaskStatus status = taskStatus(Protos.TaskState.TASK_RUNNING);
        Node node = Nodes.addNode(new Node("0"));

        // no node
        Scheduler.$.onTaskStarted(null, status);
        assertEquals(1, schedulerDriver.killedTasks.size());
        schedulerDriver.killedTasks.clear();

        // unexpected states
        for (Node.State state : Arrays.asList(Node.State.IDLE, Node.State.STOPPING)) {
            node.state = state;
            node.initRuntime(offer());

            Scheduler.$.onTaskStarted(node, status);
            assertEquals(state, node.state);

            assertEquals(1, schedulerDriver.killedTasks.size());
            schedulerDriver.killedTasks.clear();
        }

        // expected states
        for (Node.State state : Arrays.asList(Node.State.STARTING, Node.State.RUNNING, Node.State.RECONCILING)) {
            node.state = state;
            node.initRuntime(offer());

            Scheduler.$.onTaskStarted(node, status);
            assertEquals(Node.State.RUNNING, node.state);
            assertEquals(0, schedulerDriver.killedTasks.size());
        }
    }

    @Test
    public void onTaskStopped() {
        Node node = Nodes.addNode(new Node("0"));
        TaskStatus status = taskStatus(Protos.TaskState.TASK_FINISHED);

        // no node
        Scheduler.$.onTaskStopped(null, status);

        // idle
        Scheduler.$.onTaskStopped(node, status);
        assertEquals(Node.State.IDLE, node.state);

        // expected states
        for (Node.State state : Arrays.asList(Node.State.STARTING, Node.State.RUNNING, Node.State.STOPPING, Node.State.RECONCILING)) {
            node.state = state;
            node.initRuntime(offer());

            Scheduler.$.onTaskStopped(node, status);
            assertEquals(state == Node.State.STOPPING ? Node.State.IDLE : Node.State.STARTING, node.state);
            assertNull(node.runtime);
            assertNull(node.reservation);
        }
    }

    @Test
    public void launchTask() {
        Node node = Nodes.addNode(new Node("nn"));
        node.state = Node.State.STARTING;

        Scheduler.$.launchTask(node, offer());
        assertEquals(1, schedulerDriver.launchedTasks.size());

        assertEquals(Node.State.STARTING, node.state);
        assertNotNull(node.runtime);
        assertNotNull(node.reservation);
    }

    @Test
    public void checkMesosVersion() {
        // no version
        Scheduler.$.checkMesosVersion(master("id", LOCALHOST_IP, 5000, "host", ""));
        assertEquals(Protos.Status.DRIVER_STOPPED, schedulerDriver.status);

        // unsupported version
        schedulerDriver.start();
        Scheduler.$.checkMesosVersion(master("id", LOCALHOST_IP, 5000, "host", "0.22.0"));
        assertEquals(Protos.Status.DRIVER_STOPPED, schedulerDriver.status);

        // supported version
        schedulerDriver.start();
        Scheduler.$.checkMesosVersion(master("id", LOCALHOST_IP, 5000, "host", "0.23.0"));
        assertEquals(Protos.Status.DRIVER_RUNNING, schedulerDriver.status);
    }
}
