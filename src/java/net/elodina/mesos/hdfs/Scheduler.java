package net.elodina.mesos.hdfs;

import com.google.protobuf.ByteString;
import net.elodina.mesos.util.*;
import net.elodina.mesos.util.Repr;
import org.apache.log4j.*;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import static org.apache.mesos.Protos.*;

public class Scheduler implements org.apache.mesos.Scheduler {
    public static final Scheduler $ = new Scheduler();
    private static final Logger logger = Logger.getLogger(Scheduler.class);

    public Config config = new Config();
    private Reconciler reconciler = new Reconciler();

    private SchedulerDriver driver;

    @Override
    public void registered(SchedulerDriver driver, Protos.FrameworkID id, Protos.MasterInfo master) {
        logger.info("[registered] framework:" + Repr.id(id.getValue()) + " master:" + Repr.master(master));
        this.driver = driver;

        checkMesosVersion(master);
        reconciler.start(driver, new Date());

        Nodes.frameworkId = id.getValue();
        Nodes.save();
    }

    @Override
    public void reregistered(SchedulerDriver driver, Protos.MasterInfo master) {
        logger.info("[reregistered] " + Repr.master(master));

        this.driver = driver;
        reconciler.start(driver, new Date());
    }

    @Override
    public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
        logger.info("[resourceOffers]\n" + Repr.offers(offers));
        onOffers(offers);
    }

    @Override
    public void offerRescinded(SchedulerDriver driver, Protos.OfferID id) {
        logger.info("[offerRescinded] " + Repr.id(id.getValue()));
    }

    @Override
    public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
        logger.info("[statusUpdate] " + Repr.status(status));
        onTaskStatus(status);
    }

    @Override
    public void frameworkMessage(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, byte[] data) {
        logger.info("[frameworkMessage] executor:" + Repr.id(executorId.getValue()) + ", slave: " + Repr.id(slaveId.getValue()) + ", data: " + new String(data));
    }

    @Override
    public void disconnected(SchedulerDriver driver) {
        logger.info("[disconnected]");
        this.driver = null;
    }

    @Override
    public void slaveLost(SchedulerDriver driver, Protos.SlaveID id) {
        logger.info("[slaveLost] " + Repr.id(id.getValue()));
    }

    @Override
    public void executorLost(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, int status) {
        logger.info("[executorLost] executor:" + Repr.id(executorId.getValue()) + ", slave: " + Repr.id(slaveId.getValue()) + ", status: " + status);
    }

    @Override
    public void error(SchedulerDriver driver, String message) {
        logger.info("[error] " + message);
    }

    private void onOffers(List<Protos.Offer> offers) {
        // start nodes
        for (Protos.Offer offer : offers) {
            String reason = acceptOffer(offer);

            if (reason != null) {
                logger.info("Declined offer " + Repr.offer(offer) + ":\n" + reason);
                driver.declineOffer(offer.getId());
            }
        }

        // stop nodes
        for (Node node : Nodes.getNodes(Node.State.STOPPING)) {
            if (node.runtime == null) {
                node.state = Node.State.IDLE;
                continue;
            }

            if (!node.runtime.killSent) {
                driver.killTask(TaskID.newBuilder().setValue(node.runtime.taskId).build());
                node.runtime.killSent = true;
            }
        }

        reconciler.proceed(driver, new Date());
        Nodes.save();
    }

    String acceptOffer(Protos.Offer offer) {
        if (reconciler.isActive()) return "reconciling";

        List<Node> nodes = new ArrayList<>();
        for (Node node : Nodes.getNodes(Node.State.STARTING))
            if (node.runtime == null) nodes.add(node);

        if (nodes.isEmpty()) return "nothing to start";

        List<String> reasons = new ArrayList<>();
        for (Node node : nodes) {
            String reason = node.matches(offer);
            if (reason != null) reasons.add("node " + node.id + ": " + reason);
            else {
                launchTask(node, offer);
                return null;
            }
        }

        return Strings.join(reasons, ", ");
    }

    void launchTask(Node node, Protos.Offer offer) {
        node.initRuntime(offer);
        TaskInfo task = node.newTask();

        driver.launchTasks(Arrays.asList(offer.getId()), Arrays.asList(task), Filters.newBuilder().setRefuseSeconds(1).build());
        logger.info("Starting node " + node.id + " with task " + node.runtime.taskId + " for offer " + offer.getId().getValue());
    }

    void onTaskStatus(TaskStatus status) {
        Node node = getNodeByTaskId(status.getTaskId().getValue());

        switch (status.getState()) {
            case TASK_RUNNING:
                onTaskStarted(node, status);
                break;
            case TASK_FINISHED:
            case TASK_FAILED:
            case TASK_KILLED:
            case TASK_LOST:
            case TASK_ERROR:
                onTaskStopped(node, status);
        }
    }

    void onTaskStarted(Node node, TaskStatus status) {
        boolean expectedState = node != null && Arrays.asList(Node.State.STARTING, Node.State.RUNNING, Node.State.RECONCILING).contains(node.state);
        if (!expectedState) {
            String id = node != null ? node.id : "<unknown>";
            logger.info("Got " + status.getState() + " for node " + id + ", killing task");
            driver.killTask(status.getTaskId());
            return;
        }

        if (node.state == Node.State.RECONCILING)
            logger.info("Finished reconciling of node " + node.id + ", task " + node.runtime.taskId);

        node.state = Node.State.RUNNING;
    }

    void onTaskStopped(Node node, TaskStatus status) {
        boolean expectedState = node != null && node.state != Node.State.IDLE;
        if (!expectedState) {
            String id = node != null ? node.id : "<unknown>";
            logger.info("Got " + status.getState() + " for node " + id + ", ignoring it");
            return;
        }

        boolean stopping = node.state == Node.State.STOPPING;
        node.state = stopping ? Node.State.IDLE : Node.State.STARTING;
        node.runtime = null;
        node.reservation = null;
    }

    private Node getNodeByTaskId(String taskId) {
        for (Node node : Nodes.getNodes())
            if (node.runtime != null && node.runtime.taskId.equals(taskId))
                return node;

        return null;
    }

    void checkMesosVersion(MasterInfo master) {
        Version minVersion = new Version("0.23.0");
        Version version = !master.getVersion().isEmpty() ? new Version(master.getVersion()) : null;

        if (version == null || version.compareTo(minVersion) < 0) {
            String versionStr = version == null ? "?(<0.23.0)" : "" + version;
            logger.fatal("Unsupported Mesos version " + versionStr + ", expected version " + minVersion + "+");
            driver.stop();
        }
    }

    public void run() {
        initLogging();
        config.resolveDeps();

        logger.info("Starting " + getClass().getSimpleName() + ":\n" + config);
        Nodes.load();

        final HttpServer server = new HttpServer();
        try { server.start(); }
        catch (Exception e) { throw new RuntimeException(e); }

        Protos.FrameworkInfo.Builder frameworkBuilder = Protos.FrameworkInfo.newBuilder();
        if (Nodes.frameworkId != null) frameworkBuilder.setId(Protos.FrameworkID.newBuilder().setValue(Nodes.frameworkId));
        frameworkBuilder.setUser(config.user != null ? config.user : "");

        frameworkBuilder.setName(config.frameworkName);
        frameworkBuilder.setRole(config.frameworkRole);
        frameworkBuilder.setFailoverTimeout(config.frameworkTimeout.ms() / 1000);
        frameworkBuilder.setCheckpoint(true);

        Protos.Credential.Builder credsBuilder = null;
        if (config.principal != null && config.secret != null) {
            frameworkBuilder.setPrincipal(config.principal);

            credsBuilder = Protos.Credential.newBuilder();
            credsBuilder.setPrincipal(config.principal);
            credsBuilder.setSecret(ByteString.copyFromUtf8(config.secret));
        }

        MesosSchedulerDriver driver;
        if (credsBuilder != null) driver = new MesosSchedulerDriver(Scheduler.$, frameworkBuilder.build(), config.master, credsBuilder.build());
        else driver = new MesosSchedulerDriver(Scheduler.$, frameworkBuilder.build(), config.master);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info("Stopping " + getClass().getSimpleName());

                try { server.stop(); }
                catch (Exception e) { logger.warn("", e); }
            }
        });

        Protos.Status status = driver.run();
        System.exit(status == Protos.Status.DRIVER_STOPPED ? 0 : 1);
    }

    void initLogging() {
        System.setProperty("org.eclipse.jetty.util.log.class", JettyLog4jLogger.class.getName());
        BasicConfigurator.resetConfiguration();

        Logger root = Logger.getRootLogger();
        root.setLevel(Level.INFO);

        Logger.getLogger("org.eclipse.jetty").setLevel(Level.WARN);
        Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN);
        Logger.getLogger("org.I0Itec.zkclient").setLevel(Level.WARN);

        PatternLayout layout = new PatternLayout("%d [%t] %p %c{2} - %m%n");
        root.addAppender(new ConsoleAppender(layout));
    }

    public static class JettyLog4jLogger implements org.eclipse.jetty.util.log.Logger {
        private Logger logger;

        @SuppressWarnings("UnusedDeclaration")
        public JettyLog4jLogger() { this.logger = Logger.getLogger("Jetty"); }
        public JettyLog4jLogger(Logger logger) { this.logger = logger; }

        public boolean isDebugEnabled() { return logger.isDebugEnabled(); }
        public void setDebugEnabled(boolean enabled) { logger.setLevel(enabled ? Level.DEBUG : Level.INFO); }

        public void info(String s, Object... args) { logger.info(format(s, args)); }
        public void info(String s, Throwable t) { logger.info(s, t); }
        public void info(Throwable t) { logger.info("", t); }

        public void debug(String s, Object... args) { logger.debug(format(s, args)); }
        public void debug(String msg, Throwable th) { logger.debug(msg, th); }
        public void debug(Throwable t) { logger.debug("", t); }

        public void warn(String s, Object... args) { logger.warn(format(s, args)); }
        public void warn(String msg, Throwable th) { logger.warn(msg, th); }
        public void warn(Throwable t) { logger.warn("", t); }

        public void ignore(Throwable throwable) { logger.info("Ignored", throwable); }

        public org.eclipse.jetty.util.log.Logger getLogger(String name) { return new JettyLog4jLogger(Logger.getLogger(name)); }
        public String getName() { return logger.getName(); }

        private static String format(String s, Object ... args) {
            // {} text {} text ...
            String result = "";

            int i = 0;
            for (String text : s.split("\\{\\}")) {
                result += text;
                if (args.length > i) result += args[i];
                i++;
            }

            return result;
        }
    }

    public static class Config {
        public String api;
        public String storage = "file:hdfs-mesos.json";

        public File jar;
        public File hadoop;

        public int apiPort() {
            try {
                int port = new URI(api).getPort();
                return port == -1 ? 80 : port;
            } catch (URISyntaxException e) {
                throw new IllegalStateException(e);
            }
        }

        public String master;
        public String user;
        public String principal;
        public String secret;

        public String frameworkName = "hdfs";
        public String frameworkRole = "*";
        public Period frameworkTimeout = new Period("30d");

        void resolveDeps() {
            String hadoopMask = "hadoop-.*gz";
            hadoop = IO.findFile(new File("."), hadoopMask);
            if (hadoop == null) throw new IllegalStateException(hadoopMask + " not found in current dir");
            checkHadoopVersion();

            String jarMask = "hdfs-mesos-.*jar";
            jar = IO.findFile(new File("."), jarMask);
            if (jar == null) throw new IllegalStateException(jarMask + " not found in current dir");
        }

        private void checkHadoopVersion() {
            // hadoop-1.2.1.tar.gz
            String name = hadoop.getName();
            int hyphenIdx = name.indexOf("-");
            int extIdx = name.indexOf(".tar.gz");

            if (hyphenIdx == -1 || extIdx == -1) throw new IllegalStateException("Can't extract version from " + name);
            Version version = new Version(name.substring(hyphenIdx + 1, extIdx));

            boolean supported1x = version.compareTo(new Version("1.2")) >= 0 && version.compareTo(new Version("1.3")) < 0;
            boolean supported2x = version.compareTo(new Version("2.7")) >= 0 && version.compareTo(new Version("2.8")) < 0;
            if (!supported1x && !supported2x)
                throw new IllegalStateException("Supported hadoop versions are 1.2.x and 2.7.x, current is " + version);
        }

        public String toString() {
            String s = "";

            s += "api: " + api;
            s += "\nfiles: jar:" + jar + ", hadoop:" + hadoop;

            s += "\nmesos: master:" + master + ", user:" + (user == null ? "<default>" : user);
            s += ", principal:" + (principal == null ? "<none>" : principal) + ", secret:" + (secret == null ? "<none>" : "******");

            s += "\nframework: name:" + frameworkName + ", role:" + frameworkRole + ", timeout:" + frameworkTimeout;

            return s;
        }
    }

    public static class Reconciler {
        private Period delay;
        private int maxTries;

        private int tries;
        private Date lastTry;

        public Reconciler() { this(new Period("30s"), 3); }

        public Reconciler(Period delay, int maxTries) {
            this.delay = delay;
            this.maxTries = maxTries;
        }

        public Period getDelay() { return delay; }
        public int getMaxTries() { return maxTries; }

        public int getTries() { return tries; }
        public Date getLastTry() { return lastTry; }

        public boolean isActive() { return Nodes.getNodes(Node.State.RECONCILING).size() > 0; }

        public void start(SchedulerDriver driver, Date now) {
            tries = 1;
            lastTry = now;

            for (Node node : Nodes.getNodes()) {
                if (node.runtime == null) continue;

                node.state = Node.State.RECONCILING;
                logger.info("Reconciling " + tries + "/" + maxTries + " state of node " + node.id + ", task " + node.runtime.taskId);
            }

            driver.reconcileTasks(Collections.<TaskStatus>emptyList());
        }

        public void proceed(SchedulerDriver driver, Date now) {
            if (lastTry == null) return;

            if (now.getTime() - lastTry.getTime() < delay.ms())
                return;

            tries += 1;
            lastTry = now;

            if (tries > maxTries) {
                for (Node node : Nodes.getNodes(Node.State.RECONCILING)) {
                    if (node.runtime == null) continue;

                    logger.info("Reconciling exceeded " + maxTries + " tries for node " + node.id + ", sending killTask for task " + node.runtime.taskId);
                    driver.killTask(TaskID.newBuilder().setValue(node.runtime.taskId).build());
                    node.runtime = null;
                    node.state = Node.State.STARTING;
                }

                tries = 0;
                lastTry = null;
                return;
            }

            List<TaskStatus> statuses = new ArrayList<>();

            for (Node node : Nodes.getNodes(Node.State.RECONCILING)) {
                if (node.runtime == null) continue;

                logger.info("Reconciling " + tries + "/" + maxTries + " state of node " + node.id + ", task " + node.runtime.taskId);
                statuses.add(TaskStatus.newBuilder()
                    .setTaskId(TaskID.newBuilder().setValue(node.runtime.taskId))
                    .setState(TaskState.TASK_RUNNING)
                    .build()
                );
            }

            if (!statuses.isEmpty()) driver.reconcileTasks(statuses);
        }
    }
}
