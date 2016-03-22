package net.elodina.mesos.util;

public class Period {
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
