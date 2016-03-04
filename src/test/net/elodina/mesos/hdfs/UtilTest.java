package net.elodina.mesos.hdfs;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

import static net.elodina.mesos.hdfs.Util.*;
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

    // Period
    @Test
    public void Period_init() {
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
    public void Period_ms() {
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
    public void Period_value() {
        assertEquals(0, new Period("0").value());
        assertEquals(10, new Period("10ms").value());
        assertEquals(50, new Period("50h").value());
        assertEquals(20, new Period("20d").value());
    }

    @Test
    public void Period_unit() {
        assertEquals("ms", new Period("0").unit());
        assertEquals("ms", new Period("10ms").unit());
        assertEquals("h", new Period("50h").unit());
        assertEquals("d", new Period("20d").unit());
    }

    @Test
    public void Period_toString() {
        assertEquals("10ms", "" + new Period("10ms"));
        assertEquals("5h", "" + new Period("5h"));
    }

    // Version
    @Test
    public void Version_init() {
        assertEquals(Arrays.<Integer>asList(), new Version().values());
        assertEquals(Arrays.asList(1, 0), new Version(1, 0).values());
        assertEquals(Arrays.asList(1, 2, 3, 4), new Version("1.2.3.4").values());

        try { new Version(" "); fail(); }
        catch (IllegalArgumentException e) {}

        try { new Version("."); fail(); }
        catch (IllegalArgumentException e) {}

        try { new Version("a"); fail(); }
        catch (IllegalArgumentException e) {}
    }

    @Test
    public void Version_compareTo() {
        assertEquals(0, new Version().compareTo(new Version()));
        assertEquals(0, new Version(0).compareTo(new Version(0)));

        assertTrue(new Version(0).compareTo(new Version(1)) < 0);
        assertTrue(new Version(0).compareTo(new Version(0, 0)) < 0);

        assertTrue(new Version(0, 9, 0, 0).compareTo(new Version(0, 8, 2, 0)) > 0);
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
