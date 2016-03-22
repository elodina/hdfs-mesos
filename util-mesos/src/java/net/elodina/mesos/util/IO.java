package net.elodina.mesos.util;

import java.io.*;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IO {
    public static void copyAndClose(InputStream in, OutputStream out) throws IOException {
        try { copy(in, out); }
        finally {
            closeSilently(in);
            closeSilently(out);
        }
    }

    public static void copy(InputStream in, OutputStream out) throws IOException {
        byte[] buffer = new byte[128 * 1024];
        int actuallyRead = 0;

        while (actuallyRead != -1) {
            actuallyRead = in.read(buffer);
            if (actuallyRead != -1) out.write(buffer, 0, actuallyRead);
        }
    }

    public static void closeSilently(Closeable closeable) {
        try { closeable.close(); }
        catch (IOException ignore) {}
    }

    public static void delete(File file) throws IOException {
        if (file.isDirectory()) {
            File[] files = file.listFiles();

            if (files != null)
                for (File child : files) delete(child);
        }

        if (!file.delete())
            throw new IOException("Can't delete file " + file);
    }

    public static File findFile(File dir, String mask) { return findFile0(dir, mask, false); }
    public static File findDir(File dir, String mask) { return findFile0(dir, mask, true); }

    static File findFile0(File dir, String mask, boolean isDir) {
        File[] files = dir.listFiles();
        if (files == null) return null;

        for (File file : files)
            if (file.getName().matches(mask) && (isDir && file.isDirectory() || !isDir && file.isFile()))
                return file;

        return null;
    }

    public static String readFile(File file) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        copyAndClose(new FileInputStream(file), buffer);
        return buffer.toString("utf-8");
    }

    public static void writeFile(File file, String content) throws IOException {
        copyAndClose(new ByteArrayInputStream(content.getBytes("utf-8")), new FileOutputStream(file));
    }

    public static void replaceInFile(File file, Map<String, String> replacements) throws IOException { replaceInFile(file, replacements, false); }

    public static void replaceInFile(File file, Map<String, String> replacements, boolean ignoreMisses) throws IOException {
        String content = readFile(file);

        for (String regex: replacements.keySet()) {
            String value = replacements.get(regex);

            Matcher matcher = Pattern.compile(regex).matcher(content);
            if (!ignoreMisses && !matcher.find()) throw new IllegalStateException("regex $regex not found in file " + file);

            content = matcher.replaceAll(value);
        }

        writeFile(file, content);
    }
}
