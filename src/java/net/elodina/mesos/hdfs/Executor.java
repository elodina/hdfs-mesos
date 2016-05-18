package net.elodina.mesos.hdfs;

import net.elodina.mesos.api.Framework;
import net.elodina.mesos.api.Slave;
import net.elodina.mesos.api.Task;
import net.elodina.mesos.api.executor.ExecutorDriver;
import net.elodina.mesos.api.executor.ExecutorDriverV0;
import net.elodina.mesos.util.IO;
import net.elodina.mesos.util.Version;
import org.apache.log4j.*;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

public class Executor implements net.elodina.mesos.api.executor.Executor {
    public static final Logger logger = Logger.getLogger(Executor.class);

    public static File hadoopDir;
    public static Version hadoopVersion;

    public static File dataDir;
    public static File javaHome;

    public static boolean hadoop1x() { return hadoopVersion.compareTo(new Version("2.0")) < 0; }

    public static File hdfs() { return new File(hadoopDir, hadoop1x() ? "bin/hadoop" : "/bin/hdfs"); }

    public static File hadoopConfDir() { return new File(hadoopDir, hadoop1x() ? "conf" : "etc/hadoop"); }

    private ExecutorDriver driver;
    private String hostname;
    private HdfsProcess process;

    @Override
    public void registered(ExecutorDriver driver, Task.Executor executor, Framework framework, Slave slave) {
        logger.info("[registered] " + (framework != null ? "framework:" + framework.toString(true) : "") + " slave:" + slave.toString(true));
        this.driver = driver;
        hostname = slave.hostname();
    }

    @Override
    public void disconnected() {
        logger.info("[disconnected]");
        driver = null;
    }

    @Override
    public void launchTask(final Task task) {
        logger.info("[launchTask] " + task.toString(true));

        new Thread() {
            @Override
            public void run() {
                setName("ProcessRunner");

                try { runHdfs(task); }
                catch (Throwable t) {
                    logger.error("", t);

                    StringWriter buffer = new StringWriter();
                    t.printStackTrace(new PrintWriter(buffer, true));
                    driver.sendStatus(new Task.Status(task.id(), Task.State.ERROR).message("" + buffer));
                }

                driver.stop();
            }
        }.start();
    }

    private void runHdfs(Task task) throws InterruptedException, IOException {
        JSONObject json;
        try { json = (JSONObject) new JSONParser().parse(new String(task.data(), "utf-8")); }
        catch (ParseException e) { throw new IllegalStateException(e); }
        Node node = new Node(json);

        process = new HdfsProcess(node, hostname);
        process.start();
        driver.sendStatus(new Task.Status(task.id(), Task.State.STARTING));

        if (process.waitForOperable())
            driver.sendStatus(new Task.Status(task.id(), Task.State.RUNNING));

        int code = process.waitFor();
        if (code == 0 || code == 143) driver.sendStatus(new Task.Status(task.id(), Task.State.FINISHED));
        else driver.sendStatus(new Task.Status(task.id(), Task.State.FAILED).message("process exited with " + code));
    }

    @Override
    public void killTask(String id) {
        logger.info("[killTask] " + id);
        if (process != null) process.stop();
    }

    @Override
    public void message(byte[] data) {
        logger.info("[message] " + new String(data));
    }

    @Override
    public void shutdown() {
        logger.info("[shutdown]");
    }

    @Override
    public void error(String message) {
        logger.info("[error] " + message);
    }

    public static void main(String[] args) {
        initLogging();
        initDirs();

        ExecutorDriverV0 driver = new ExecutorDriverV0(new Executor());
        boolean ok = driver.run();
        System.exit(ok ? 0 : 1);
    }

    static void initDirs() {
        String hadoopMask = "hadoop-.*";
        hadoopDir = IO.findDir(new File("."), hadoopMask);
        if (hadoopDir == null) throw new IllegalStateException(hadoopMask + " not found in current dir");

        int hyphenIdx = hadoopDir.getName().lastIndexOf("-");
        if (hyphenIdx == -1) throw new IllegalStateException("Can't extract version from " + hadoopDir);
        hadoopVersion = new Version(hadoopDir.getName().substring(hyphenIdx + 1));

        dataDir = new File(new File("."), "data");
        javaHome = findJavaHome();

        logger.info("Resolved dirs:\nhadoopDir=" + hadoopDir + "\ndataDir=" + dataDir + "\njavaHome=" + javaHome);
    }

    static File findJavaHome() {
        File jreDir = IO.findDir(new File("."), "jre.*");
        if (jreDir != null) return jreDir;

        if (System.getenv("JAVA_HOME") != null)
            return new File(System.getenv("JAVA_HOME"));

        if (System.getenv("PATH") != null)
            for (String part : System.getenv("PATH").split(":")) {
                part = part.trim();
                if (part.startsWith("\"") && part.endsWith("\""))
                    part = part.substring(1, part.length() - 1);

                File java = new File(part, "java");
                if (java.isFile() && java.canRead()) {
                    File dir = javaHomeDir(java);
                    if (dir != null) return dir;
                }
            }

        throw new IllegalStateException("Can't resolve JAVA_HOME / find jre");
    }

    private static File javaHomeDir(File java) {
        try {
            File tmpFile = File.createTempFile("java_home", null);

            Process process = new ProcessBuilder("readlink", "-f", java.getAbsolutePath())
                .redirectError(ProcessBuilder.Redirect.INHERIT)
                .redirectOutput(tmpFile).start();

            int code = process.waitFor();
            if (code != 0) throw new IOException("Process exited with code " + code);

            File file = new File(IO.readFile(tmpFile).trim()); // $JRE_PATH/bin/java
            if (!tmpFile.delete()) throw new IOException("Failed to delete " + tmpFile);

            file = file.getParentFile();
            if (file != null) file = file.getParentFile();
            return file;
        } catch (IOException | InterruptedException e) {
            logger.warn("", e);
            return null;
        }
    }

    static void initLogging() {
        BasicConfigurator.resetConfiguration();

        Logger root = Logger.getRootLogger();
        root.setLevel(Level.INFO);

        Logger.getLogger("net.elodina.mesos.api").setLevel(Level.DEBUG);

        PatternLayout layout = new PatternLayout("[executor] %d [%t] %p %c{2} - %m%n");
        root.addAppender(new ConsoleAppender(layout));
    }
}
