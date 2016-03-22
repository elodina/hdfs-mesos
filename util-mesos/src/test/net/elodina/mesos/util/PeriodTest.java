package net.elodina.mesos.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class PeriodTest {
    @Test
    public void init() {
        new Period("1m");

        // empty
        try { new Period(""); fail(); }
        catch (IllegalArgumentException ignore) {}

        // zero without units
        new Period("0");

        // no units
        try { new Period("1"); fail(); }
        catch (IllegalArgumentException ignore) {}

        // no value
        try { new Period("ms"); fail(); }
        catch (IllegalArgumentException ignore) {}

        // wrong unit
        try { new Period("1k"); fail(); }
        catch (IllegalArgumentException ignore) {}

        // non-integer value
        try { new Period("0.5m"); fail(); }
        catch (IllegalArgumentException ignore) {}

        // invalid value
        try { new Period("Xh"); fail(); }
        catch (IllegalArgumentException ignore) {}
    }

    @Test
    public void ms() {
        assertEquals(0, new Period("0").ms());
        assertEquals(1, new Period("1ms").ms());
        assertEquals(10, new Period("10ms").ms());

        int s = 1000;
        assertEquals(s, new Period("1s").ms());
        assertEquals(10 * s, new Period("10s").ms());

        int m = 60 * s;
        assertEquals(m, new Period("1m").ms());
        assertEquals(10 * m, new Period("10m").ms());

        int h = 60 * m;
        assertEquals(h, new Period("1h").ms());
        assertEquals(10 * h, new Period("10h").ms());

        int d = 24 * h;
        assertEquals(d, new Period("1d").ms());
        assertEquals(10 * d, new Period("10d").ms());
    }

    @Test
    public void value() {
        assertEquals(0, new Period("0").value());
        assertEquals(10, new Period("10ms").value());
        assertEquals(50, new Period("50h").value());
        assertEquals(20, new Period("20d").value());
    }

    @Test
    public void unit() {
        assertEquals("ms", new Period("0").unit());
        assertEquals("ms", new Period("10ms").unit());
        assertEquals("h", new Period("50h").unit());
        assertEquals("d", new Period("20d").unit());
    }

    @Test
    public void _toString() {
        assertEquals("10ms", "" + new Period("10ms"));
        assertEquals("5h", "" + new Period("5h"));
    }
}
