package net.elodina.mesos.test;

import com.google.protobuf.ByteString;
import net.elodina.mesos.util.Range;
import net.elodina.mesos.util.Strings;
import org.junit.Ignore;

import java.util.*;

import static org.apache.mesos.Protos.*;

@Ignore
public class MesosTestCase {
    protected TestSchedulerDriver schedulerDriver = new TestSchedulerDriver();
    protected TestExecutorDriver executorDriver = new TestExecutorDriver();

    public FrameworkID frameworkId() { return frameworkId("" + UUID.randomUUID()); }
    public FrameworkID frameworkId(String id) { return FrameworkID.newBuilder().setValue(id).build(); }

    public ExecutorID executorId() { return executorId("" + UUID.randomUUID()); }
    public ExecutorID executorId(String id) { return ExecutorID.newBuilder().setValue(id).build(); }

    public SlaveID slaveId() { return slaveId("" + UUID.randomUUID()); }
    public SlaveID slaveId(String id) { return SlaveID.newBuilder().setValue(id).build(); }

    public TaskID taskId() { return taskId("" + UUID.randomUUID()); }
    public TaskID taskId(String id) { return TaskID.newBuilder().setValue(id).build(); }

    protected static final int LOCALHOST_IP = 2130706433;
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
    public Offer offer(String resources) { return offer("host", resources); }
    public Offer offer(String hostname, String resources) { return offer("" + UUID.randomUUID(), "" + UUID.randomUUID(), "" + UUID.randomUUID(), hostname, resources, null); }
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

    public TaskInfo task() { return task("" + UUID.randomUUID(), "Task", "" + UUID.randomUUID(), ""); }
    public TaskInfo task(String id, String name, String slaveId, String data) {
        TaskInfo.Builder builder = TaskInfo.newBuilder()
            .setName(name)
            .setTaskId(TaskID.newBuilder().setValue(id))
            .setSlaveId(SlaveID.newBuilder().setValue(slaveId));

        if (data != null) builder.setData(ByteString.copyFromUtf8(data));

        return builder.build();
    }

    public TaskStatus taskStatus(TaskState state) { return taskStatus("" + UUID.randomUUID(), state, null); }
    public TaskStatus taskStatus(String id, TaskState state) { return taskStatus(id, state, ""); }
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
}
