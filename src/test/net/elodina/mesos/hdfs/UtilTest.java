package net.elodina.mesos.hdfs;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static net.elodina.mesos.hdfs.Util.IO;
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

    // IO
    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void IO_findFile0() throws IOException {
        File dir = Files.createTempDirectory(UtilTest.class.getSimpleName()).toFile();

        try {
            assertNull(IO.findDir(dir, "mask.*"));

            File matchedFile = new File(dir, "mask-123");
            matchedFile.createNewFile();

            assertNull(IO.findFile0(dir, "mask.*", true));
            assertEquals(matchedFile, IO.findFile0(dir, "mask.*", false));
        } finally {
            IO.delete(dir);
        }
    }

    @Test
    public void IO_replaceInFile() throws IOException {
        File file = Files.createTempFile(UtilTest.class.getSimpleName(), null).toFile();

        Map<String, String> map = new HashMap<>();
        map.put("a=*.", "a=1");
        map.put("c=*.", "c=3");

        IO.writeFile(file, "a=0\nb=0\nc=0");
        IO.replaceInFile(file, map);
        assertEquals("a=1\nb=0\nc=3", IO.readFile(file));

        // error on miss
        IO.writeFile(file, "a=0\nb=0");
        try { IO.replaceInFile(file, map); }
        catch (IllegalStateException e) { assertTrue(e.getMessage(), e.getMessage().contains("not found in file")); }

        // ignore misses
        IO.writeFile(file, "a=0\nb=0");
        IO.replaceInFile(file, map, true);
        assertEquals("a=1\nb=0", IO.readFile(file));
    }
}
