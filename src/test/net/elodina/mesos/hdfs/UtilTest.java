package net.elodina.mesos.hdfs;

import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class UtilTest {
    @Test
    public void parseMap() {
        Map<String, String> map = Util.parseMap("a=1,b=2");
        assertEquals(2, map.size());
        assertEquals("1", map.get("a"));
        assertEquals("2", map.get("b"));

        // missing pair
        try { Util.parseMap("a=1,,b=2"); fail(); }
        catch (IllegalArgumentException e) {}

        // null value
        map = Util.parseMap("a=1,b,c=3");
        assertEquals(3, map.size());
        assertNull(map.get("b"));

        try { Util.parseMap("a=1,b,c=3", false); }
        catch (IllegalArgumentException e) {}

        // escaping
        map = Util.parseMap("a=\\,,b=\\=,c=\\\\");
        assertEquals(3, map.size());
        assertEquals(",", map.get("a"));
        assertEquals("=", map.get("b"));
        assertEquals("\\", map.get("c"));

        // open escaping
        try { Util.parseMap("a=\\"); fail(); }
        catch (IllegalArgumentException e) {}

        // null
        assertTrue(Util.parseMap(null).isEmpty());
    }

    @Test
    public void formatMap() {
        Map<String, String> map = new LinkedHashMap<>();
        map.put("a", "1");
        map.put("b", "2");
        assertEquals("a=1,b=2", Util.formatMap(map));

        // null value
        map.put("b", null);
        assertEquals("a=1,b", Util.formatMap(map));

        // escaping
        map.put("a", ",");
        map.put("b", "=");
        map.put("c", "\\");
        assertEquals("a=\\,,b=\\=,c=\\\\", Util.formatMap(map));
    }
}
