package net.elodina.mesos.hdfs;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import static net.elodina.mesos.hdfs.Util.IO;
import static net.elodina.mesos.hdfs.Util.Period;
import static org.junit.Assert.*;

public class UtilTest {
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
