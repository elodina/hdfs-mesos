package net.elodina.mesos.hdfs;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

import static net.elodina.mesos.hdfs.Util.IO;
import static net.elodina.mesos.hdfs.Util.Range;
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

    // Range
    @Test
    public void Range_init() {
        new Range("30");
        new Range("30..31");
        new Range(30);
        new Range(30, 31);

        // empty
        try { new Range(""); fail(); }
        catch (IllegalArgumentException e) {}

        // non int
        try { new Range("abc"); fail(); }
        catch (IllegalArgumentException e) {}

        // non int first
        try { new Range("abc..30"); fail(); }
        catch (IllegalArgumentException e) {}

        // non int second
        try { new Range("30..abc"); fail(); }
        catch (IllegalArgumentException e) {}

        // inverted range
        try { new Range("10..0"); fail(); }
        catch (IllegalArgumentException e) {}
    }

    @Test
    public void Range_start_end() {
        assertEquals(0, new Range("0").start());
        assertEquals(0, new Range("0..10").start());
        assertEquals(10, new Range("0..10").end());
    }

    @Test
    public void Range_overlap() {
        // no overlap
        assertNull(new Range(0, 10).overlap(new Range(20, 30)));
        assertNull(new Range(20, 30).overlap(new Range(0, 10)));
        assertNull(new Range(0).overlap(new Range(1)));

        // partial
        assertEquals(new Range(5, 10), new Range(0, 10).overlap(new Range(5, 15)));
        assertEquals(new Range(5, 10), new Range(5, 15).overlap(new Range(0, 10)));

        // includes
        assertEquals(new Range(2, 3), new Range(0, 10).overlap(new Range(2, 3)));
        assertEquals(new Range(2, 3), new Range(2, 3).overlap(new Range(0, 10)));
        assertEquals(new Range(5), new Range(0, 10).overlap(new Range(5)));

        // last point
        assertEquals(new Range(0), new Range(0, 10).overlap(new Range(0)));
        assertEquals(new Range(10), new Range(0, 10).overlap(new Range(10)));
        assertEquals(new Range(0), new Range(0).overlap(new Range(0)));
    }

    @Test
    public void Range_contains() {
        assertTrue(new Range(0).contains(0));
        assertTrue(new Range(0,1).contains(0));
        assertTrue(new Range(0,1).contains(1));

        Range range = new Range(100, 200);
        assertTrue(range.contains(100));
        assertTrue(range.contains(150));
        assertTrue(range.contains(200));

        assertFalse(range.contains(99));
        assertFalse(range.contains(201));
    }

    @Test
    public void Range_split() {
        assertEquals(Collections.<Range>emptyList(), new Range(0).split(0));

        assertEquals(Arrays.asList(new Range(1)), new Range(0, 1).split(0));
        assertEquals(Arrays.asList(new Range(0)), new Range(0, 1).split(1));

        assertEquals(Arrays.asList(new Range(0), new Range(2)), new Range(0, 2).split(1));
        assertEquals(Arrays.asList(new Range(100, 149), new Range(151, 200)), new Range(100, 200).split(150));

        try { new Range(100, 200).split(10); fail(); }
        catch (IllegalArgumentException e) {}

        try { new Range(100, 200).split(210); fail(); }
        catch (IllegalArgumentException e) {}
    }

    @Test
    public void Range_toString() {
        assertEquals("0", "" + new Range("0"));
        assertEquals("0..10", "" + new Range("0..10"));
        assertEquals("0", "" + new Range("0..0"));
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
