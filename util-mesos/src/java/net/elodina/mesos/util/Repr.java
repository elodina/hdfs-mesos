package net.elodina.mesos.util;

import org.apache.mesos.Protos;

import java.text.SimpleDateFormat;
import java.util.*;

@SuppressWarnings("UnusedDeclaration")
public class Repr {
    public static String dateTime(Date date) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ssX").format(date);
    }

    public static String framework(Protos.FrameworkInfo framework) {
        String s = "";

        s += id(framework.getId().getValue());
        s += " name: " + framework.getName();
        s += " hostname: " + framework.getHostname();
        s += " failover_timeout: " + framework.getFailoverTimeout();

        return s;
    }

    public static String master(Protos.MasterInfo master) {
        String s = "";

        s += id(master.getId());
        s += " pid:" + master.getPid();
        s += " hostname:" + master.getHostname();

        return s;
    }

    public static String slave(Protos.SlaveInfo slave) {
        String s = "";

        s += id(slave.getId().getValue());
        s += " hostname:" + slave.getHostname();
        s += " port:" + slave.getPort();
        s += " " + resources(slave.getResourcesList());

        return s;
    }

    public static String offer(Protos.Offer offer) {
        String s = "";

        s += offer.getHostname() + id(offer.getId().getValue());
        s += " " + resources(offer.getResourcesList());
        if (offer.getAttributesCount() > 0) s += " " + attributes(offer.getAttributesList());

        return s;
    }

    public static String offers(Iterable<Protos.Offer> offers) {
        String s = "";

        for (Protos.Offer offer : offers)
            s += (s.isEmpty() ? "" : "\n") + offer(offer);

        return s;
    }

    public static String task(Protos.TaskInfo task) {
        String s = "";

        s += task.getTaskId().getValue();
        s += " slave:" + id(task.getSlaveId().getValue());

        s += " " + resources(task.getResourcesList());
        s += " data:" + new String(task.getData().toByteArray());

        return s;
    }

    public static String resources(List<Protos.Resource> resources) {
        String s = "";

        final List<String> order = Arrays.asList("cpus mem disk ports".split(" "));
        resources = new ArrayList<>(resources);
        Collections.sort(resources, new Comparator<Protos.Resource>() {
            public int compare(Protos.Resource x, Protos.Resource y) {
                return order.indexOf(x.getName()) - order.indexOf(y.getName());
            }
        });

        for (Protos.Resource resource : resources) {
            if (!s.isEmpty()) s += "; ";

            s += resource.getName();
            if (!resource.getRole().equals("*")) {
                s += "(" + resource.getRole();

                if (resource.hasReservation() && resource.getReservation().hasPrincipal())
                    s += ", " + resource.getReservation().getPrincipal();

                s += ")";
            }

            if (resource.hasDisk())
                s += "[" + resource.getDisk().getPersistence().getId() + ":" + resource.getDisk().getVolume().getContainerPath() + "]";

            s += ":";

            if (resource.hasScalar())
                s += String.format("%.2f", resource.getScalar().getValue());

            if (resource.hasRanges())
                for (Protos.Value.Range range : resource.getRanges().getRangeList())
                    s += "[" + range.getBegin() + ".." + range.getEnd() + "]";
        }

        return s;
    }


    public static String attributes(List<Protos.Attribute> attributes) {
        String s = "";

        for (Protos.Attribute attr : attributes) {
            if (!s.isEmpty()) s += ";";
            s += attr.getName() + ":";

            if (attr.hasText()) s += attr.getText().getValue();
            if (attr.hasScalar()) s += String.format("%.2f", attr.getScalar().getValue());
        }

        return s;
    }

    public static String status(Protos.TaskStatus status) {
        String s = "";
        s += status.getTaskId().getValue();
        s += " " + status.getState().name();

        s += " slave:" + id(status.getSlaveId().getValue());

        if (status.getState() != Protos.TaskState.TASK_RUNNING)
            s += " reason:" + status.getReason().name();

        if (status.getMessage() != null && !status.getMessage().isEmpty())
            s += " message:" + status.getMessage();

        if (status.getData().size() > 0)
            s += " data: " + status.getData().toStringUtf8();

        return s;
    }

    public static String id(String id) { return "#" + suffix(id, 5); }

    public static String suffix(String s, int maxLen) { return s.length() <= maxLen ? s : s.substring(s.length() - maxLen); }
}
