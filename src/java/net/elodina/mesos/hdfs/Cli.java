package net.elodina.mesos.hdfs;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.json.simple.JSONAware;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Cli {
    static String api = "http://localhost:7000";
    static PrintStream out = System.out;
    static PrintStream err = System.err;

    public static void main(String... args) {
        try {
            handle(new ArrayList<>(Arrays.asList(args)));
        } catch (Error e) {
            err.println("Error: " + e.getMessage());
            System.exit(1);
        }
    }

    static void handle(List<String> args) {
        if (args.isEmpty()) throw new Error("command required");

        String cmd = args.remove(0);
        if (!cmd.equals("help")) args = handleGenericOptions(args, false);

        switch (cmd) {
            case "help": handleHelp(args); break;
            case "scheduler": SchedulerCli.handle(args, false); break;
            case "node": NodeCli.handle(args, false); break;
            default: throw new Error("unsupported command " + cmd);
        }
    }

    private static void handleHelp(List<String> args) {
        String cmd = args.isEmpty() ? null : args.remove(0);
        if (cmd == null) {
            printLine("Usage: <cmd> ...\n");
            printCmds();

            printLine();
            printLine("Run `help <cmd>` to see details of specific command");
            return;
        }

        switch (cmd) {
            case "help":
                printLine("Print general or command-specific help\nUsage: help [cmd [cmd]]");
                break;
            case "scheduler": SchedulerCli.handle(args, true); break;
            case "node": NodeCli.handle(args, true); break;
            default: throw new Error("unsupported command " + cmd);
        }
    }

    private static void printCmds() {
        printLine("Commands:");
        printLine("help [cmd [cmd]] - print general or command-specific help", 1);
        printLine("scheduler        - start scheduler", 1);
        printLine("node             - node management", 1);
    }


    static List<String> handleGenericOptions(List<String> args, boolean help) {
        OptionParser parser = new OptionParser();
        parser.accepts("api", "Binding host:port for http/artifact server. Optional if HM_API env is set.")
            .withOptionalArg().ofType(String.class);

        parser.allowsUnrecognizedOptions();

        if (help) {
            printLine("Generic Options");

            try { parser.printHelpOn(out); }
            catch (IOException ignore) {}

            return args;
        }

        OptionSet options;
        try { options = parser.parse(args.toArray(new String[args.size()])); }
        catch (OptionException e) {
            try { parser.printHelpOn(out); }
            catch (IOException ignore) {}

            printLine();
            throw new Error(e.getMessage());
        }

        resolveApi((String) options.valueOf("api"));

        @SuppressWarnings("unchecked") List<String> result = (List<String>) options.nonOptionArguments();
        return new ArrayList<>(result);
    }

    private static void resolveApi(String api) {
        if (api != null && !api.equals("")) {
            Scheduler.$.config.api = api;
            return;
        }

        if (System.getenv("HM_API") != null) {
            Scheduler.$.config.api = System.getenv("HM_API");
            return;
        }

        throw new Error("Undefined API url. Please provide either a CLI --api option or HM_API env.");
    }

    static JSONAware sendRequest(String uri, Map<String, String> params) throws IOException {
        String qs = queryString(params);
        String url = api + (api.endsWith("/") ? "" : "/") + "api" + uri;

        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        String response = null;
        try {
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);

            byte[] requestBody = qs.getBytes("utf-8");
            connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded; charset=utf-8");
            connection.setRequestProperty("Content-Length", "" + requestBody.length);
            connection.getOutputStream().write(requestBody);

            try {
                ByteArrayOutputStream responseBody = new ByteArrayOutputStream();
                Util.IO.copyAndClose(connection.getInputStream(), responseBody);
                response = responseBody.toString("utf-8");
            } catch (IOException e) {
                if (connection.getResponseCode() != 200) throw new IOException(connection.getResponseCode() + " - " + connection.getResponseMessage());
                else throw e;
            }
        } finally {
            connection.disconnect();
        }

        JSONAware json;
        try { json = (JSONAware) new JSONParser().parse(response); }
        catch (ParseException e) { throw new IOException(e); }

        return json;
    }

    private static String queryString(Map<String, String> params) {
        String s = "";

        for (String name : params.keySet()) {
            String value = params.get(name);
            if (!s.isEmpty()) s += "&";

            try {
                s += URLEncoder.encode(name, "utf-8");
                if (value != null) s += "=" + URLEncoder.encode(value, "utf-8");
            } catch (UnsupportedEncodingException ignore) {}
        }

        return s;
    }

    static void printLine() { printLine(""); }

    static void printLine(String s) { printLine(s, 0); }

    static void printLine(String s, int indent) {
        char[] c = new char[2 * indent];
        Arrays.fill(c, ' ');
        out.println(new String(c) + s);
    }

    static class Error extends RuntimeException {
        Error(String message) {
            super(message);
        }
    }
}
