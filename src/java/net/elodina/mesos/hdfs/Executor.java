package net.elodina.mesos.hdfs;

import org.apache.log4j.*;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;

import static net.elodina.mesos.hdfs.Util.Str;
import static org.apache.mesos.Protos.*;

public class Executor implements org.apache.mesos.Executor {
    public static final Logger logger = Logger.getLogger(Executor.class);
    public static File hadoopDir;
    public static File dataDir;

    private NNProcess process;

    @Override
    public void registered(ExecutorDriver driver, ExecutorInfo executorInfo, FrameworkInfo framework, SlaveInfo slave) {
        logger.info("[registered] framework:" + Str.framework(framework) + " slave:" + Str.slave(slave));
    }

    @Override
    public void reregistered(ExecutorDriver driver, SlaveInfo slave) {
        logger.info("[registered] " + Str.slave(slave));
    }

    @Override
    public void disconnected(ExecutorDriver driver) {
        logger.info("[disconnected]");
    }

    @Override
    public void launchTask(final ExecutorDriver driver, final TaskInfo task) {
        logger.info("[launchTask] " + Str.task(task));

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

    private void runHdfs(TaskInfo task, ExecutorDriver driver) throws InterruptedException {
        process = new NNProcess();
        process.start();

        driver.sendStatusUpdate(TaskStatus.newBuilder().setTaskId(task.getTaskId()).setState(TaskState.TASK_RUNNING).build());
        int code = process.waitFor();

        if (code == 0) driver.sendStatusUpdate(TaskStatus.newBuilder().setTaskId(task.getTaskId()).setState(TaskState.TASK_FINISHED).build());
        else driver.sendStatusUpdate(TaskStatus.newBuilder().setTaskId(task.getTaskId()).setState(TaskState.TASK_FAILED).setMessage("process exited with " + code).build());
    }

    @Override
    public void killTask(ExecutorDriver driver, TaskID id) {
        logger.info("[launchTask] " + Str.id(id.getValue()));
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
        hadoopDir = Util.IO.findDir(new File("."), "hadoop-.*");
        dataDir = new File(new File("."), "data");
    }

    static void initLogging() {
        BasicConfigurator.resetConfiguration();

        Logger root = Logger.getRootLogger();
        root.setLevel(Level.INFO);

        PatternLayout layout = new PatternLayout("%d [%t] %-5p %c %x - %m%n");
        root.addAppender(new ConsoleAppender(layout));
    }
}
