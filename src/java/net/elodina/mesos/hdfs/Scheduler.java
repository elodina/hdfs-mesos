package net.elodina.mesos.hdfs;

import net.elodina.mesos.api.*;
import net.elodina.mesos.util.IO;
import net.elodina.mesos.util.Period;
import net.elodina.mesos.util.Strings;
import net.elodina.mesos.util.Version;
import org.apache.log4j.*;

import java.io.File;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class Scheduler extends net.elodina.mesos.api.Scheduler {
    public static final Scheduler $ = new Scheduler();
    private static final Logger logger = Logger.getLogger(Scheduler.class);

    public Config config = new Config();
    private Reconciler reconciler = new Reconciler();

    private Scheduler.Driver driver;

    @Override
    public void registered(Scheduler.Driver driver, String id, Master master) {
        logger.info("[registered] framework:" + id + " master:[" + master + "]");
        this.driver = driver;

        checkMesosVersion(master);
        reconciler.start(driver, new Date());

        Nodes.frameworkId = id;
        Nodes.save();
    }

    @Override
    public void reregistered(Scheduler.Driver driver, Master master) {
        logger.info("[reregistered] " + master);

        this.driver = driver;
        reconciler.start(driver, new Date());
    }

    @Override
    public void offers(List<Offer> offers) {
        logger.info("[resourceOffers]\n" + offers);
        onOffers(offers);
    }

    @Override
    public void status(Task.Status status) {
        logger.info("[statusUpdate] " + status);
        onTaskStatus(status);
    }

    @Override
    public void message(String executorId, String slaveId, byte[] data) {
        logger.info("[frameworkMessage] executor:" + executorId + ", slave: " + slaveId + ", data: " + new String(data));
    }

    @Override
    public void disconnected() {
        logger.info("[disconnected]");
        this.driver = null;
    }

    private void onOffers(List<Offer> offers) {
        // start nodes
        for (Offer offer : offers) {
            String reason = acceptOffer(offer);

            if (reason != null) {
                logger.info("Declined offer " + offer + ":\n" + reason);
                driver.declineOffer(offer.id());
            }
        }

        // stop nodes
        for (Node node : Nodes.getNodes(Node.State.STOPPING)) {
            if (node.runtime == null) {
                node.state = Node.State.IDLE;
                continue;
            }

            if (!node.runtime.killSent) {
                driver.killTask(node.runtime.taskId);
                node.runtime.killSent = true;
            }
        }

        reconciler.proceed(driver, new Date());
        Nodes.save();
    }

    String acceptOffer(Offer offer) {
        if (reconciler.isActive()) return "reconciling";

        List<Node> nodes = new ArrayList<>();
        for (Node node : Nodes.getNodes(Node.State.STARTING))
            if (node.runtime == null) nodes.add(node);

        if (nodes.isEmpty()) return "nothing to start";

        List<String> reasons = new ArrayList<>();
        for (Node node : nodes) {
            String reason = node.matches(offer, otherAttributes());
            if (reason != null) reasons.add("node " + node.id + ": " + reason);
            else {
                launchTask(node, offer);
                return null;
            }
        }

        return Strings.join(reasons, ", ");
    }

    void launchTask(Node node, Offer offer) {
        node.initRuntime(offer);
        Task task = node.newTask();

        driver.launchTask(offer.id(), task);
        logger.info("Starting node " + node.id + " with task " + node.runtime.taskId + " for offer " + offer.id());
    }

    void onTaskStatus(Task.Status status) {
        Node node = getNodeByTaskId(status.id());

        switch (status.state()) {
            case RUNNING:
                onTaskStarted(node, status);
                break;
            case FINISHED:
            case FAILED:
            case KILLED:
            case LOST:
            case ERROR:
                onTaskStopped(node, status);
        }
    }

    void onTaskStarted(Node node, Task.Status status) {
        boolean expectedState = node != null && Arrays.asList(Node.State.STARTING, Node.State.RUNNING, Node.State.RECONCILING).contains(node.state);
        if (!expectedState) {
            String id = node != null ? node.id : "<unknown>";
            logger.info("Got " + status.state() + " for node " + id + ", killing task");
            driver.killTask(status.id());
            return;
        }

        if (node.state == Node.State.RECONCILING)
            logger.info("Finished reconciling of node " + node.id + ", task " + node.runtime.taskId);

        node.state = Node.State.RUNNING;
    }

    void onTaskStopped(Node node, Task.Status status) {
        boolean expectedState = node != null && node.state != Node.State.IDLE;
        if (!expectedState) {
            String id = node != null ? node.id : "<unknown>";
            logger.info("Got " + status.state() + " for node " + id + ", ignoring it");
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

    Map<String, Collection<String>> otherAttributes() {
        class Result {
            Map<String, Collection<String>> map = new HashMap<>();
            void add(String name, String value) {
                if (!map.containsKey(name)) map.put(name, new ArrayList<String>());
                map.get(name).add(value);
            }
        }
        Result result = new Result();

        for (Node node : Nodes.getNodes()) {
            if (node.runtime == null) continue;

            result.add("hostname", node.runtime.hostname);
            for (String name : node.runtime.attributes.keySet())
                result.add(name, node.runtime.attributes.get(name));
        }

        return result.map;
    }

    void checkMesosVersion(Master master) {
        if (master == null) return;
        Version minVersion = new Version("0.23.0");
        Version version = master.version();

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

        Framework framework = new Framework();

        if (Nodes.frameworkId != null) framework.id(Nodes.frameworkId);
        framework.user(config.user != null ? config.user : "");

        framework.name(config.frameworkName);
        framework.role(config.frameworkRole);
        framework.timeout(config.frameworkTimeout);
        framework.checkpoint(true);

        Cred cred = null;
        if (config.principal != null && config.secret != null) {
            framework.principal(config.principal);
            cred = new Cred(config.principal, config.secret);
        }

        Driver driver = new SchedulerDriverV1(Scheduler.$, framework, config.master);
        driver.setDebug(new PrintWriter(System.err, true)); // todo fix me?
//        Driver driver = new TcpV0Driver(Scheduler.$, framework, config.master, cred);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info("Stopping " + getClass().getSimpleName());

                try { server.stop(); }
                catch (Exception e) { logger.warn("", e); }
            }
        });

        boolean stopped;
        try { stopped = driver.run(); }
        catch (Exception e) { throw new Error(e); }
        System.exit(stopped ? 0 : 1);
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

        public void start(Scheduler.Driver driver, Date now) {
            tries = 1;
            lastTry = now;

            for (Node node : Nodes.getNodes()) {
                if (node.runtime == null) continue;

                node.state = Node.State.RECONCILING;
                logger.info("Reconciling " + tries + "/" + maxTries + " state of node " + node.id + ", task " + node.runtime.taskId);
            }

            driver.reconcileTasks(Collections.<String>emptyList());
        }

        public void proceed(Scheduler.Driver driver, Date now) {
            if (lastTry == null) return;

            if (now.getTime() - lastTry.getTime() < delay.ms())
                return;

            tries += 1;
            lastTry = now;

            if (tries > maxTries) {
                for (Node node : Nodes.getNodes(Node.State.RECONCILING)) {
                    if (node.runtime == null) continue;

                    logger.info("Reconciling exceeded " + maxTries + " tries for node " + node.id + ", sending killTask for task " + node.runtime.taskId);
                    driver.killTask(node.runtime.taskId);
                    node.runtime = null;
                    node.state = Node.State.STARTING;
                }

                tries = 0;
                lastTry = null;
                return;
            }

            List<String> ids = new ArrayList<>();

            for (Node node : Nodes.getNodes(Node.State.RECONCILING)) {
                if (node.runtime == null) continue;
                logger.info("Reconciling " + tries + "/" + maxTries + " state of node " + node.id + ", task " + node.runtime.taskId);
                ids.add(node.runtime.taskId);
            }

            if (!ids.isEmpty()) driver.reconcileTasks(ids);
        }
    }
}
