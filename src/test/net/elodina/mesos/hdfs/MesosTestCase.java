package net.elodina.mesos.hdfs;

import com.google.protobuf.ByteString;
import net.elodina.mesos.util.Net;
import net.elodina.mesos.util.Range;
import net.elodina.mesos.util.Strings;
import org.apache.log4j.BasicConfigurator;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.SchedulerDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

import java.io.File;
import java.nio.file.Files;
import java.util.*;

import static org.apache.mesos.Protos.*;
import static org.junit.Assert.assertTrue;

@Ignore
public class MesosTestCase {
    TestSchedulerDriver schedulerDriver;
    TestExecutorDriver executorDriver;

    @Before
    public void before() throws Exception {
        BasicConfigurator.configure();
        Scheduler.$.initLogging();

        File storageFile = Files.createTempFile(MesosTestCase.class.getSimpleName(), null).toFile();
        assertTrue(storageFile.delete());
        Nodes.storage = Storage.file(storageFile);
        Nodes.reset();

        schedulerDriver = new TestSchedulerDriver();
        Scheduler.$.registered(schedulerDriver, frameworkId(), master());

        executorDriver = new TestExecutorDriver();

        Scheduler.Config config = Scheduler.$.config;
        config.api = "http://localhost:" + Net.findAvailPort();
        config.jar = new File("hdfs-mesos-0.1.jar");
        config.hadoop = new File("hadoop-1.2.1.tar.gz");

        Cli.api = config.api;
    }

    @After
    public void after() throws Exception {
        Scheduler.$.disconnected(schedulerDriver);
        BasicConfigurator.resetConfiguration();

        Scheduler.Config config = Scheduler.$.config;
        config.api = null;
        config.jar = null;

        Nodes.storage.clear();
    }

    public FrameworkID frameworkId() { return frameworkId("" + UUID.randomUUID()); }
    public FrameworkID frameworkId(String id) { return FrameworkID.newBuilder().setValue(id).build(); }

    public TaskID taskId() { return taskId("" + UUID.randomUUID()); }
    public TaskID taskId(String id) { return TaskID.newBuilder().setValue(id).build(); }

    static final int LOCALHOST_IP = 2130706433;
    public MasterInfo master() { return master("" + UUID.randomUUID(), LOCALHOST_IP, 5050, "master", "0.23.0"); }
    public MasterInfo master(String id, int ip, int port, String hostname, String version) {
        return MasterInfo.newBuilder()
            .setId(id)
            .setIp(ip)
            .setPort(port)
            .setHostname(hostname)
            .setVersion(version)
            .build();
    }

    public Offer offer() { return offer("cpus:1;mem:1024;ports:0..100"); }
    public Offer offer(String resources) { return offer("" + UUID.randomUUID(), "" + UUID.randomUUID(), "" + UUID.randomUUID(), "host", resources, null); }
    public Offer offer(String id, String frameworkId, String slaveId, String hostname, String resources, String attributes) {
        Offer.Builder builder = Offer.newBuilder()
            .setId(OfferID.newBuilder().setValue(id))
            .setFrameworkId(FrameworkID.newBuilder().setValue(frameworkId))
            .setSlaveId(SlaveID.newBuilder().setValue(slaveId));

        builder.setHostname(hostname);
        builder.addAllResources(this.resources(resources));

        if (attributes != null) {
            Map<String, String> map = Strings.parseMap(attributes);

            for (String k : map.keySet()) {
                String v = map.get(k);

                Attribute attribute = Attribute.newBuilder()
                    .setType(Value.Type.TEXT)
                    .setName(k)
                    .setText(Value.Text.newBuilder().setValue(v))
                    .build();

                builder.addAttributes(attribute);
            }
        }

        return builder.build();
    }

    public TaskInfo task() { return task("" + UUID.randomUUID(), "Task", "" + UUID.randomUUID(), Strings.formatMap(Collections.singletonMap("node", new Node().toJson()))); }
    public TaskInfo task(String id, String name, String slaveId, String data) {
        TaskInfo.Builder builder = TaskInfo.newBuilder()
            .setName(name)
            .setTaskId(TaskID.newBuilder().setValue(id))
            .setSlaveId(SlaveID.newBuilder().setValue(slaveId));

        if (data != null) builder.setData(ByteString.copyFromUtf8(data));

        return builder.build();
    }

    public TaskStatus taskStatus(TaskState state) { return taskStatus("" + UUID.randomUUID(), state, null); }
    public TaskStatus taskStatus(String id, TaskState state, String data) {
        TaskStatus.Builder builder = TaskStatus.newBuilder()
            .setTaskId(TaskID.newBuilder().setValue(id))
            .setState(state);

        if (data != null)
            builder.setData(ByteString.copyFromUtf8(data));

        return builder.build();
    }

    // parses range definition: 1000..1100,1102,2000..3000
    public List<Value.Range> ranges(String s) {
        if (s.isEmpty()) return Collections.emptyList();

        List<Value.Range> result = new ArrayList<>();
        for (String part : s.split(",")) {
            part = part.trim();
            Range r = new Range(part);
            result.add(Value.Range.newBuilder().setBegin(r.start()).setEnd(r.end()).build());
        }

        return result;
    }

    // parses resources definition like: cpus:0.5; cpus(kafka):0.3; mem:128; ports(kafka):1000..2000
    // Must parse the following
    // disk:73390
    // disk(*):73390
    // disk(kafka):73390
    // cpu(kafka, principal):0.01
    // disk(kafka, principal)[test_volume:fake_path]:100)
    public List<Resource> resources(String s) {
        List<Resource> resources = new ArrayList<>();
        if (s == null) return resources;

        for (String r : s.split(";")) {
            r = r.trim();
            if (r.isEmpty()) continue;

            int colonIdx = r.lastIndexOf(":");
            if (colonIdx == -1) throw new IllegalArgumentException("invalid resource: " + r);
            String key = r.substring(0, colonIdx);

            String role = "*";
            String principal = null;
            String volumeId = null;
            String volumePath = null;

            // role & principal
            int roleStart = key.indexOf("(");
            if (roleStart != -1) {
                int roleEnd = key.indexOf(")");
                if (roleEnd == -1) throw new IllegalArgumentException(s);

                role = key.substring(roleStart + 1, roleEnd);

                int principalIdx = role.indexOf(",");
                if (principalIdx != -1) {
                    principal = role.substring(principalIdx + 1);
                    role = role.substring(0, principalIdx);
                }

                key = key.substring(0, roleStart) + key.substring(roleEnd + 1);
            }

            // volume
            int volumeStart = key.indexOf("[");
            if (volumeStart != -1) {
                int volumeEnd = key.indexOf("]");
                if (volumeEnd == -1) throw new IllegalArgumentException(s);

                String volume = key.substring(volumeStart + 1, volumeEnd);
                int volColonIdx = volume.indexOf(":");

                volumeId = volume.substring(0, volColonIdx);
                volumePath = volume.substring(volColonIdx + 1);

                key = key.substring(0, volumeStart) + key.substring(volumeEnd + 1);
            }

            // name & value
            String name = key;
            String value = r.substring(colonIdx + 1);

            Resource.Builder builder = Resource.newBuilder()
                .setName(name)
                .setRole(role);

            if (principal != null)
                builder.setReservation(Resource.ReservationInfo.newBuilder().setPrincipal(principal));

            if (volumeId != null)
                builder.setDisk(Resource.DiskInfo.newBuilder()
                    .setPersistence(Resource.DiskInfo.Persistence.newBuilder().setId(volumeId))
                    .setVolume(Volume.newBuilder().setContainerPath(volumePath).setMode(Volume.Mode.RW))
                );

            switch (key) {
                case "cpus": case "mem": case "disk":
                    builder.setType(Value.Type.SCALAR).setScalar(Value.Scalar.newBuilder().setValue(Double.parseDouble(value)));
                    break;
                case "ports":
                    builder.setType(Value.Type.RANGES).setRanges(Value.Ranges.newBuilder().addAllRange(ranges(value)));
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported resource type: " + key);
            }

            resources.add(builder.build());
        }

        return resources;
    }

    public static class TestSchedulerDriver implements SchedulerDriver {
        public Status status = Status.DRIVER_RUNNING;

        public List<String> declinedOffers = new ArrayList<>();
        public List<String> acceptedOffers = new ArrayList<>();

        public List<TaskInfo> launchedTasks = new ArrayList<>();
        public List<String> killedTasks = new ArrayList<>();
        public List<String> reconciledTasks = new ArrayList<>();

        public Status declineOffer(OfferID id) {
            declinedOffers.add(id.getValue());
            return status;
        }

        public Status declineOffer(OfferID id, Filters filters) {
            declinedOffers.add(id.getValue());
            return status;
        }

        public Status launchTasks(OfferID offerId, Collection<TaskInfo> tasks) {
            acceptedOffers.add(offerId.getValue());
            launchedTasks.addAll(tasks);
            return status;
        }

        public Status launchTasks(OfferID offerId, Collection<TaskInfo> tasks, Filters filters) {
            acceptedOffers.add(offerId.getValue());
            launchedTasks.addAll(tasks);
            return status;
        }

        public Status launchTasks(Collection<OfferID> offerIds, Collection<TaskInfo> tasks) {
            for (OfferID offerId : offerIds) acceptedOffers.add(offerId.getValue());
            launchedTasks.addAll(tasks);
            return status;
        }

        public Status launchTasks(Collection<OfferID> offerIds, Collection<TaskInfo> tasks, Filters filters) {
            for (OfferID offerId : offerIds) acceptedOffers.add(offerId.getValue());
            launchedTasks.addAll(tasks);
            return status;
        }

        public Status stop() { return status = Status.DRIVER_STOPPED; }

        public Status stop(boolean failover) { return status = Status.DRIVER_STOPPED; }

        public Status killTask(TaskID id) {
            killedTasks.add(id.getValue());
            return status;
        }

        public Status requestResources(Collection<Request> requests) { throw new UnsupportedOperationException(); }

        public Status sendFrameworkMessage(ExecutorID executorId, SlaveID slaveId, byte[] data) { throw new UnsupportedOperationException(); }

        public Status join() { throw new UnsupportedOperationException(); }

        public Status reconcileTasks(Collection<TaskStatus> statuses) {
            if (statuses.isEmpty()) reconciledTasks.add("");
            for (TaskStatus status : statuses) reconciledTasks.add(status.getTaskId().getValue());
            return status;
        }

        public Status reviveOffers() { throw new UnsupportedOperationException(); }

        public Status run() { return status = Status.DRIVER_RUNNING; }

        public Status abort() { return status = Status.DRIVER_ABORTED; }

        public Status start() { return status = Status.DRIVER_RUNNING; }

        public Status acceptOffers(Collection<OfferID> offerIds, Collection<Offer.Operation> operations, Filters filters) { throw new UnsupportedOperationException(); }

        public Status acknowledgeStatusUpdate(TaskStatus status) { throw new UnsupportedOperationException(); }

        public Status suppressOffers() { throw new UnsupportedOperationException(); }
    }

    static class TestExecutorDriver implements ExecutorDriver {
        public Status status = Status.DRIVER_RUNNING;
        public final List<TaskStatus> statusUpdates = new ArrayList<>();

        public Status start() {
            status = Status.DRIVER_RUNNING;
            return status;
        }

        public Status stop() {
            status = Status.DRIVER_STOPPED;
            return status;
        }

        public Status abort() {
            status = Status.DRIVER_ABORTED;
            return status;
        }

        public Status join() { return status; }

        public Status run() {
            status = Status.DRIVER_RUNNING;
            return status;
        }

        public Status sendStatusUpdate(TaskStatus status) {
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

        public Status sendFrameworkMessage(byte[] message) { throw new UnsupportedOperationException(); }
    }
}
