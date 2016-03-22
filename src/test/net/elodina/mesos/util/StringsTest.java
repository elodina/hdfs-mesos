package net.elodina.mesos.util;

import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class StringsTest {
    @Test
    public void capitalize() {
        assertEquals("", Strings.capitalize(""));
        assertEquals("123", Strings.capitalize("123"));

        assertEquals("A", Strings.capitalize("a"));
        assertEquals("Abc", Strings.capitalize("abc"));
    }

    @Test
    public void join() {
        assertEquals("", Strings.join(Arrays.asList(), ","));
        assertEquals("1,2,3", Strings.join(Arrays.asList(1,2,3), ","));
    }

    @Test
    public void parseMap() {
        Map<String, String> map = Strings.parseMap("a=1,b=2");
        assertEquals(2, map.size());
        assertEquals("1", map.get("a"));
        assertEquals("2", map.get("b"));

        // missing pair
        try { Strings.parseMap("a=1,,b=2"); fail(); }
        catch (IllegalArgumentException e) {}

        // null value
        map = Strings.parseMap("a=1,b,c=3");
        assertEquals(3, map.size());
        assertNull(map.get("b"));

        try { Strings.parseMap("a=1,b,c=3", false); }
        catch (IllegalArgumentException e) {}

        // escaping
        map = Strings.parseMap("a=\\,,b=\\=,c=\\\\");
        assertEquals(3, map.size());
        assertEquals(",", map.get("a"));
        assertEquals("=", map.get("b"));
        assertEquals("\\", map.get("c"));

        // open escaping
        try { Strings.parseMap("a=\\"); fail(); }
        catch (IllegalArgumentException e) {}

        // null
        assertTrue(Strings.parseMap(null).isEmpty());
    }

    @Test
    public void formatMap() {
        Map<String, String> map = new LinkedHashMap<>();
        map.put("a", "1");
        map.put("b", "2");
        assertEquals("a=1,b=2", Strings.formatMap(map));

        // null value
        map.put("b", null);
        assertEquals("a=1,b", Strings.formatMap(map));

        // escaping
        map.put("a", ",");
        map.put("b", "=");
        map.put("c", "\\");
        assertEquals("a=\\,,b=\\=,c=\\\\", Strings.formatMap(map));
    }
}
