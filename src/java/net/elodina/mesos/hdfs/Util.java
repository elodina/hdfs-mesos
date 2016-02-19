package net.elodina.mesos.hdfs;

import org.apache.mesos.Protos;

import java.io.*;
import java.net.ServerSocket;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    public static class Range {
        private int start = -1;
        private int end = -1;

        public Range(int start) { this(start, start); }
        public Range(int start, int end) {
            this.start = start;
            this.end = end;
        }

        public Range(String s) {
            int idx = s.indexOf("..");

            if (idx == -1) {
                start = Integer.parseInt(s);
                end = start;
                return;
            }

            start = Integer.parseInt(s.substring(0, idx));
            end = Integer.parseInt(s.substring(idx + 2));
            if (start > end) throw new IllegalArgumentException("start > end");
        }

        public int start() { return start; }
        public int end() { return end; }

        @SuppressWarnings("SuspiciousNameCombination")
        public Range overlap(Range r) {
            Range x = this;
            Range y = r;

            if (x.start > y.start) {
                Range t = x;
                x = y;
                y = t;
            }
            assert x.start <= y.start;

            if (y.start > x.end) return null;
            assert y.start <= x.end;

            int start = y.start;
            int end = Math.min(x.end, y.end);
            return new Range(start, end);
        }

        public boolean contains(int p) { return start <= p && p <= end; }

        public List<Range> split(int p) {
            if (!contains(p)) throw new IllegalArgumentException("point not in range");

            List<Range> result = new ArrayList<>();
            if (start < p) result.add(new Range(start, p - 1));
            if (p < end) result.add(new Range(p + 1, end));

            return result;
        }

        public boolean equals(Object obj) {
            if (!(obj instanceof Range)) return false;
            Range range = (Range) obj;
            return start == range.start && end == range.end;
        }

        public int hashCode() { return 31 * start + end; }

        public String toString() { return start == end ? "" + start : start + ".." + end; }
    }

    public static class Period {
        private long value;
        private String unit;
        private long ms;

        public Period(String s) {
            if (s.isEmpty()) throw new IllegalArgumentException(s);

            int unitIdx = s.length() - 1;
            if (s.endsWith("ms")) unitIdx -= 1;
            if (s.equals("0")) unitIdx = 1;

            try { value = Long.parseLong(s.substring(0, unitIdx)); }
            catch (IllegalArgumentException e) { throw new IllegalArgumentException(s); }

            unit = s.substring(unitIdx);
            if (s.equals("0")) unit = "ms";

            ms = value;
            switch (unit) {
                case "ms": ms *= 1; break;
                case "s": ms *= 1000; break;
                case "m": ms *= 60 * 1000; break;
                case "h": ms *= 60 * 60 * 1000; break;
                case "d": ms *= 24 * 60 * 60 * 1000; break;
                default: throw new IllegalArgumentException(s);
            }
        }

        public long value() { return value; }
        public String unit() { return unit; }
        public long ms() { return ms; }

        public boolean equals(Object obj) { return obj instanceof Period && ms == ((Period) obj).ms; }
        public int hashCode() { return new Long(ms).hashCode(); }
        public String toString() { return value + unit; }
    }

    public static class IO {
        public static void copyAndClose(InputStream in, OutputStream out) throws IOException {
            byte[] buffer = new byte[128 * 1024];
            int actuallyRead = 0;

            try {
                while (actuallyRead != -1) {
                    actuallyRead = in.read(buffer);
                    if (actuallyRead != -1) out.write(buffer, 0, actuallyRead);
                }
            } finally {
                try { in.close(); }
                catch (IOException ignore) {}

                try { out.close(); }
                catch (IOException ignore) {}
            }
        }

        public static void delete(File file) throws IOException {
            if (file.isDirectory()) {
                File[] files = file.listFiles();

                if (files != null)
                    for (File child : files) delete(child);
            }

            if (!file.delete())
                throw new IOException("Can't delete file " + file);
        }

        public static File findFile(File dir, String mask) { return findFile0(dir, mask, false); }
        public static File findDir(File dir, String mask) { return findFile0(dir, mask, true); }

        static File findFile0(File dir, String mask, boolean isDir) {
            File[] files = dir.listFiles();
            if (files == null) return null;

            for (File file : files)
                if (file.getName().matches(mask) && (isDir && file.isDirectory() || !isDir && file.isFile()))
                    return file;

            return null;
        }

        public static String readFile(File file) throws IOException {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            copyAndClose(new FileInputStream(file), buffer);
            return buffer.toString("utf-8");
        }

        public static void writeFile(File file, String content) throws IOException {
            copyAndClose(new ByteArrayInputStream(content.getBytes("utf-8")), new FileOutputStream(file));
        }

        public static void replaceInFile(File file, Map<String, String> replacements) throws IOException { replaceInFile(file, replacements, false); }

        public static void replaceInFile(File file, Map<String, String> replacements, boolean ignoreMisses) throws IOException {
            String content = readFile(file);

            for (String regex: replacements.keySet()) {
                String value = replacements.get(regex);

                Matcher matcher = Pattern.compile(regex).matcher(content);
                if (!ignoreMisses && !matcher.find()) throw new IllegalStateException("regex $regex not found in file " + file);

                content = matcher.replaceAll(value);
            }

            writeFile(file, content);
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
