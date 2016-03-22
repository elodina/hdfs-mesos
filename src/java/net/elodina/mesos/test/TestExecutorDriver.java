package net.elodina.mesos.test;

import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;

import java.util.ArrayList;
import java.util.List;

public class TestExecutorDriver implements ExecutorDriver {
    public Protos.Status status = Protos.Status.DRIVER_RUNNING;
    public final List<Protos.TaskStatus> statusUpdates = new ArrayList<>();

    public Protos.Status start() {
        status = Protos.Status.DRIVER_RUNNING;
        return status;
    }

    public Protos.Status stop() {
        status = Protos.Status.DRIVER_STOPPED;
        return status;
    }

    public Protos.Status abort() {
        status = Protos.Status.DRIVER_ABORTED;
        return status;
    }

    public Protos.Status join() { return status; }

    public Protos.Status run() {
        status = Protos.Status.DRIVER_RUNNING;
        return status;
    }

    public Protos.Status sendStatusUpdate(Protos.TaskStatus status) {
        synchronized (statusUpdates) {
            statusUpdates.add(status);
            statusUpdates.notify();
        }

        return this.status;
    }

    public void waitForStatusUpdates(int count) throws InterruptedException {
        synchronized (statusUpdates) {
            while (statusUpdates.size() < count)
                statusUpdates.wait();
        }
    }

    public Protos.Status sendFrameworkMessage(byte[] message) { throw new UnsupportedOperationException(); }
}
