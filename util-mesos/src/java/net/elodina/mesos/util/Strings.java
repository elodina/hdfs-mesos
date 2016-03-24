package net.elodina.mesos.util;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Strings {
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

    public static Map<String, String> parseMap(String s) { return parseMap(s, false); }

    public static Map<String, String> parseMap(String s, boolean nullValues) { return parseMap(s, ',', '=', nullValues); }

    public static Map<String, String> parseMap(String s, char entrySep) { return parseMap(s, entrySep, '=', false); }

    public static Map<String, String> parseMap(String s, char entrySep, char valueSep) { return parseMap(s, entrySep, valueSep, false); }

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
}
