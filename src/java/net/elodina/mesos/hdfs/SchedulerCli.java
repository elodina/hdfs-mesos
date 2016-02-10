package net.elodina.mesos.hdfs;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import java.io.IOException;
import java.util.List;

import static net.elodina.mesos.hdfs.Cli.out;
import static net.elodina.mesos.hdfs.Cli.printLine;
import static net.elodina.mesos.hdfs.Cli.Error;

public class SchedulerCli {
    public static void handle(List<String> args, boolean help) {
        OptionParser parser = new OptionParser();
        parser.accepts("master", "Mesos Master addresses.").withRequiredArg().required().ofType(String.class);
        parser.accepts("user", "Mesos user. Default - none").withRequiredArg().ofType(String.class);

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

        String master = (String) options.valueOf("master");
        String user = (String) options.valueOf("user");

        Scheduler.$.config.master = master;
        Scheduler.$.config.user = user;

        Scheduler.$.run();
    }
}
