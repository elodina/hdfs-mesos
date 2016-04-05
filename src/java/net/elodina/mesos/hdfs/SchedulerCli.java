package net.elodina.mesos.hdfs;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import net.elodina.mesos.util.Period;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static net.elodina.mesos.hdfs.Cli.Error;
import static net.elodina.mesos.hdfs.Cli.*;

public class SchedulerCli {
    public static void handle(List<String> args, boolean help) {
        Scheduler.Config config = Scheduler.$.config;

        OptionParser parser = new OptionParser();
        parser.accepts("api", "Binding host:port for http/artifact server.").withRequiredArg().ofType(String.class);
        parser.accepts("storage", " Storage for cluster state.\nDefault - " + config.storage + ".\nExamples:\n  file:hdfs-mesos.json;\n  zk:master:2181/hdfs-mesos;\n  zk:m1:2181,m2:2181/hdfs-mesos;").withRequiredArg().ofType(String.class);

        parser.accepts("master", "Mesos Master addresses.").withRequiredArg().ofType(String.class);
        parser.accepts("user", "Mesos user. Default - none").withRequiredArg().ofType(String.class);

        parser.accepts("framework-name", "Framework name. Default - " + config.frameworkName + ".").withRequiredArg().ofType(String.class);
        parser.accepts("framework-role", "Framework role. Default- " + config.frameworkRole + ".").withRequiredArg().ofType(String.class);
        parser.accepts("framework-timeout", "Framework failover timeout. Default - " + config.frameworkTimeout + ".").withRequiredArg().ofType(String.class);

        if (help) {
            printLine("Generic Options");

            try { parser.printHelpOn(out); }
            catch (IOException ignore) {}

            return;
        }

        OptionSet options;
        try { options = parser.parse(args.toArray(new String[args.size()])); }
        catch (OptionException e) {
            try { parser.printHelpOn(out); }
            catch (IOException ignore) {}

            printLine();
            throw new Error(e.getMessage());
        }

        Map<String, String> defaults = defaults();

        String api = (String) options.valueOf("api");
        if (api == null) api = defaults.get("api");
        if (api == null) throw new Error("api required");

        String storage = (String) options.valueOf("storage");
        if (storage == null) storage = defaults.get("storage");
        if (storage != null)
            try { Storage.byUri(storage); }
            catch (IllegalArgumentException e) { throw new Error("invalid storage"); }

        String master = (String) options.valueOf("master");
        if (master == null) master = defaults.get("master");
        if (master == null) throw new Error("master required");

        String user = (String) options.valueOf("user");
        if (user == null) user = defaults.get("user");

        String frameworkName = (String) options.valueOf("framework-name");
        if (frameworkName == null) frameworkName = defaults.get("framework-name");

        String frameworkRole = (String) options.valueOf("framework-role");
        if (frameworkRole == null) frameworkRole = defaults.get("framework-role");

        String frameworkTimeout = (String) options.valueOf("framework-timeout");
        if (frameworkTimeout == null) frameworkTimeout = defaults.get("framework-timeout");
        if (frameworkTimeout != null)
            try { new Period(frameworkTimeout); }
            catch (IllegalArgumentException e) { throw new Error("invalid framework-timeout"); }

        config.api = api;
        if (storage != null) config.storage = storage;

        config.master = master;
        config.user = user;

        if (frameworkName != null) config.frameworkName = frameworkName;
        if (frameworkRole != null) config.frameworkRole = frameworkRole;
        if (frameworkTimeout != null) config.frameworkTimeout = new Period(frameworkTimeout);

        Scheduler.$.run();
    }

    private static Map<String, String> defaults() {
        Map<String, String> defaults = new HashMap<>();

        File file = new File("hdfs-mesos.properties");
        if (!file.exists()) return defaults;

        Properties props = new Properties();
        try (InputStream stream = new FileInputStream(file)) { props.load(stream); }
        catch (IOException e) { throw new IOError(e); }

        for (Object name : props.keySet())
            defaults.put("" + name, props.getProperty("" + name));
        return defaults;
    }
}
