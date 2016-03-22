package net.elodina.mesos.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Version implements Comparable<Version> {
    private List<Integer> values;

    public Version(Integer... values) {
        this.values = Arrays.asList(values);
    }

    public Version(String s) {
        values = new ArrayList<>();
        if (!s.isEmpty())
            for (String part : s.split("\\.", -1))
                values.add(Integer.parseInt(part));
    }

    public List<Integer> values() { return Collections.unmodifiableList(values); }

    public int compareTo(Version v) {
        for (int i = 0; i < Math.min(values.size(), v.values.size()); i++) {
            int diff = values.get(i) - v.values.get(i);
            if (diff != 0) return diff;
        }

        return values.size() - v.values.size();
    }

    public int hashCode() { return values.hashCode(); }

    public boolean equals(Object obj) {
        return obj instanceof Version && values.equals(((Version) obj).values);
    }

    public String toString() { return Strings.join(values, "."); }
}
