package net.elodina.mesos.hdfs;

import org.apache.mesos.Protos;

import java.io.IOError;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.*;

public class Util {
    public static String capitalize(String s) {
        if (s.isEmpty()) return s;
        char[] chars = s.toCharArray();
        chars[0] = Character.toUpperCase(chars[0]);
        return new String(chars);
    }

    public static String join(Iterable<?> objects, String separator) {
        String result = "";

        for (Object object : objects)
            result += object + separator;

        if (result.length() > 0)
            result = result.substring(0, result.length() - separator.length());

        return result;
    }

    public static Map<String, String> parseMap(String s) { return parseMap(s, true); }
    public static Map<String, String> parseMap(String s, boolean nullValues) { return parseMap(s, ',', '=', nullValues); }
    public static Map<String, String> parseMap(String s, char entrySep, char valueSep, boolean nullValues) {
        Map<String, String> result = new LinkedHashMap<>();
        if (s == null) return result;

        for (String entry : splitEscaped(s, entrySep, false)) {
            if (entry.trim().isEmpty()) throw new IllegalArgumentException(s);

            List<String> pair = splitEscaped(entry, valueSep, true);
            String key = pair.get(0).trim();
            String value = pair.size() > 1 ? pair.get(1).trim() : null;

            if (value == null && !nullValues) throw new IllegalArgumentException(s);
            result.put(key, value);
        }

        return result;
    }

    private static List<String> splitEscaped(String s, char sep, boolean unescape) {
        List<String> parts = new ArrayList<>();

        boolean escaped = false;
        String part = "";
        for (char c : s.toCharArray()) {
            if (c == '\\' && !escaped) escaped = true;
            else if (c == sep && !escaped) {
                parts.add(part);
                part = "";
            } else {
                if (escaped && !unescape) part += "\\";
                part += c;
                escaped = false;
            }
        }

        if (escaped) throw new IllegalArgumentException("open escaping");
        if (!part.equals("")) parts.add(part);

        return parts;
    }

    public static String formatMap(Map<String, ?> map) { return formatMap(map, ',', '='); }
    public static String formatMap(Map<String, ?> map, char entrySep, char valueSep) {
        String s = "";

        for (String k : map.keySet()) {
            Object v = map.get(k);
            if (!s.isEmpty()) s += entrySep;
            s += escape(k, entrySep, valueSep);
            if (v != null) s += valueSep + escape("" + v, entrySep, valueSep);
        }

        return s;
    }

    private static String escape(String s, char entrySep, char valueSep) {
        String result = "";

        for (char c : s.toCharArray()) {
            if (c == entrySep || c == valueSep || c == '\\') result += "\\";
            result += c;
        }

        return result;
    }


    public static int findAvailPort() {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public static boolean isPortOpen(String host, int port) {
        try (Socket socket = new Socket(host, port)) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public static class Str {
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
}
