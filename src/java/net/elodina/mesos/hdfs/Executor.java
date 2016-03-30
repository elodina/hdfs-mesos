package net.elodina.mesos.hdfs;

import net.elodina.mesos.util.IO;
import net.elodina.mesos.util.Repr;
import net.elodina.mesos.util.Version;
import org.apache.log4j.*;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import static org.apache.mesos.Protos.*;

public class Executor implements org.apache.mesos.Executor {
    public static final Logger logger = Logger.getLogger(Executor.class);

    public static File hadoopDir;
    public static Version hadoopVersion;

    public static File dataDir;
    public static File javaHome;

    public static boolean hadoop2x() { return hadoopVersion.compareTo(new Version("2.0")) >= 0; }

    public static File hdfs() { return new File(hadoopDir, hadoop2x() ? "/bin/hdfs" : "bin/hadoop"); }

    public static File hadoopConfDir() { return new File(hadoopDir, hadoop2x() ? "etc/hadoop" : "conf"); }

    private String hostname;
    private HdfsProcess process;

    @Override
    public void registered(ExecutorDriver driver, ExecutorInfo executorInfo, FrameworkInfo framework, SlaveInfo slave) {
        logger.info("[registered] framework:" + Repr.framework(framework) + " slave:" + Repr.slave(slave));
        hostname = slave.getHostname();
    }

    @Override
    public void reregistered(ExecutorDriver driver, SlaveInfo slave) {
        logger.info("[registered] " + Repr.slave(slave));
    }

    @Override
    public void disconnected(ExecutorDriver driver) {
        logger.info("[disconnected]");
    }

    @Override
    public void launchTask(final ExecutorDriver driver, final TaskInfo task) {
        logger.info("[launchTask] " + Repr.task(task));

        new Thread() {
            @Override
            public void run() {
                setName("ProcessRunner");

                try { runHdfs(task, driver); }
                catch (Throwable t) {
                    logger.error("", t);

                    StringWriter buffer = new StringWriter();
                    t.printStackTrace(new PrintWriter(buffer, true));
                    driver.sendStatusUpdate(TaskStatus.newBuilder().setTaskId(task.getTaskId()).setState(TaskState.TASK_ERROR).setMessage("" + buffer).build());
                }

                driver.stop();
            }
        }.start();
    }

    private void runHdfs(TaskInfo task, ExecutorDriver driver) throws InterruptedException, IOException {
        JSONObject json;
        try { json = (JSONObject) new JSONParser().parse(task.getData().toStringUtf8()); }
        catch (ParseException e) { throw new IllegalStateException(e); }
        Node node = new Node(json);

        process = new HdfsProcess(node, hostname);
        process.start();
        driver.sendStatusUpdate(TaskStatus.newBuilder().setTaskId(task.getTaskId()).setState(TaskState.TASK_STARTING).build());

        if (process.waitForOperable())
            driver.sendStatusUpdate(TaskStatus.newBuilder().setTaskId(task.getTaskId()).setState(TaskState.TASK_RUNNING).build());

        int code = process.waitFor();
        if (code == 0 || code == 143) driver.sendStatusUpdate(TaskStatus.newBuilder().setTaskId(task.getTaskId()).setState(TaskState.TASK_FINISHED).build());
        else driver.sendStatusUpdate(TaskStatus.newBuilder().setTaskId(task.getTaskId()).setState(TaskState.TASK_FAILED).setMessage("process exited with " + code).build());
    }

    @Override
    public void killTask(ExecutorDriver driver, TaskID id) {
        logger.info("[killTask] " + Repr.id(id.getValue()));
        if (process != null) process.stop();
    }

    @Override
    public void frameworkMessage(ExecutorDriver driver, byte[] data) {
        logger.info("[frameworkMessage] " + new String(data));
    }

    @Override
    public void shutdown(ExecutorDriver driver) {
        logger.info("[shutdown]");
    }

    @Override
    public void error(ExecutorDriver driver, String message) {
        logger.info("[error] " + message);
    }

    public static void main(String[] args) {
        initLogging();
        initDirs();

        MesosExecutorDriver driver = new MesosExecutorDriver(new Executor());
        Status status = driver.run();

        int code = status == Status.DRIVER_STOPPED ? 0 : 1;
        System.exit(code);
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

        PatternLayout layout = new PatternLayout("[executor] %d [%t] %p %c{2} - %m%n");
        root.addAppender(new ConsoleAppender(layout));
    }
}
