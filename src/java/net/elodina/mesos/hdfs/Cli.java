package net.elodina.mesos.hdfs;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.json.simple.JSONAware;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.*;

public class Cli {
    static String api;
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
        if (!cmd.equals("help") && !cmd.equals("scheduler"))
            args = handleGenericOptions(args, false);

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
        parser.accepts("api", "REST api url (same as --api option for scheduler).")
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
        if (Cli.api != null) return;

        if (api != null && !api.equals("")) {
            Cli.api = api;
            return;
        }

        if (System.getenv("HM_API") != null) {
            Cli.api = System.getenv("HM_API");
            return;
        }

        File file = new File("hdfs-mesos.properties");
        if (file.exists()) {
            Properties props = new Properties();
            try (InputStream stream = new FileInputStream(file)) { props.load(stream); }
            catch (IOException e) { throw new IOError(e); }

            Cli.api = props.getProperty("api");
            if (Cli.api != null) return;
        }

        throw new Error("Undefined API url. Please provide one of following: CLI --api option, HM_API env var, api var in hdfs-mesos.properties.");
    }

    static <T extends JSONAware> T sendRequest(String uri, Map<String, String> params) throws IOException {
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

        if (response.trim().isEmpty()) return null;

        JSONAware json;
        try { json = (JSONAware) new JSONParser().parse(response); }
        catch (ParseException e) { throw new IOException(e); }

        @SuppressWarnings("unchecked") T result = (T) json;
        return result;
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
