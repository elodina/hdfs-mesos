package net.elodina.mesos.hdfs;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.json.simple.JSONAware;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.util.*;

import static net.elodina.mesos.hdfs.Cli.Error;
import static net.elodina.mesos.hdfs.Cli.*;

public class NodeCli {
    public static void handle(List<String> args, boolean help) {
        if (help) {
            handleHelp(args);
            return;
        }

        if (args.isEmpty()) throw new Error("command required");
        String cmd = args.remove(0);

        switch (cmd) {
            case "list": handleList(false); break;
            case "add": case "update": handleAddUpdate(cmd, args, false); break;
            case "start": case "stop": handleStartStop(cmd, args, false); break;
            default: throw new Error("unsupported command " + cmd);
        }
    }

    private static void handleHelp(List<String> args) {
        String cmd = args.isEmpty() ? null : args.remove(0);

        if (cmd == null) {
            printLine("Node management commands\nUsage: node <cmd>\n");
            printCmds();

            printLine();
            printLine("Run `help node <cmd>` to see details of specific command");
            return;
        }

        switch (cmd) {
            case "list": handleList(true); break;
            case "add": case "update": handleAddUpdate(cmd, args, true); break;
            case "start": case "stop": handleStartStop(cmd, args, true); break;
            default: throw new Error("unsupported command " + cmd);
        }
    }

    private static void handleList(boolean help) {
        if (help) {
            printLine("List nodes\nUsage: node list\n");
            handleGenericOptions(null, true);
            return;
        }

        JSONAware json;
        try { json = sendRequest("/node/list", Collections.<String, String>emptyMap()); }
        catch (IOException e) { throw new Error("" + e); }

        @SuppressWarnings("unchecked") List<JSONObject> nodesJson = (List<JSONObject>) json;
        List<Node> nodes = new ArrayList<>();
        for (JSONObject nodeJson : nodesJson) nodes.add(new Node(nodeJson));

        String title = nodes.isEmpty() ? "no nodes" : "node" + (nodes.size() > 1 ? "s" : "") + ":";
        printLine(title);

        for (Node node : nodes) {
            printNode(node, 1);
            printLine();
        }
    }

    private static void handleAddUpdate(String cmd, List<String> args, boolean help) {
        OptionParser parser = new OptionParser();
        if (cmd.equals("add")) parser.accepts("type", "node type (name_node, data_node).").withRequiredArg().required().ofType(String.class);

        parser.accepts("cpus", "CPU amount (0.5, 1, 2).").withRequiredArg().ofType(Double.class);
        parser.accepts("mem", "Mem amount in Mb.").withRequiredArg().ofType(Long.class);

        if (help) {
            printLine(Util.capitalize(cmd) + " node \nUsage: node " + cmd + " <id> [options]\n");
            try { parser.printHelpOn(out); }
            catch (IOException ignore) {}

            printLine();
            handleGenericOptions(args, true);
            return;
        }

        if (args.isEmpty()) throw new Error("id required");
        String id = args.remove(0);

        OptionSet options;
        try { options = parser.parse(args.toArray(new String[args.size()])); }
        catch (OptionException e) {
            try { parser.printHelpOn(out); }
            catch (IOException ignore) {}

            printLine();
            throw new Error(e.getMessage());
        }

        String type = (String) options.valueOf("type");
        Double cpus = (Double) options.valueOf("cpus");
        Long mem = (Long) options.valueOf("mem");

        Map<String, String> params = new HashMap<>();
        params.put("node", id);

        if (type != null) params.put("type", type);
        if (cpus != null) params.put("cpus", "" + cpus);
        if (mem != null) params.put("mem", "" + mem);

        JSONAware json;
        try { json = sendRequest("/node/" + cmd, params); }
        catch (IOException e) { throw new Error("" + e); }

        @SuppressWarnings("unchecked") List<JSONObject> nodesJson = (List<JSONObject>) json;
        List<Node> nodes = new ArrayList<>();
        for (JSONObject nodeJson : nodesJson) nodes.add(new Node(nodeJson));

        String title = "node" + (nodes.size() > 1 ? "s" : "") + (cmd.equals("add") ? " added" : " updated") + ":";
        printLine(title);

        for (Node node : nodes) {
            printNode(node, 1);
            printLine();
        }
    }

    private static void handleStartStop(String cmd, List<String> args, boolean help) {
        OptionParser parser = new OptionParser();
        parser.accepts("timeout", "timeout (30s, 1m, 1h). 0s - no timeout").withRequiredArg().ofType(String.class);

        if (help) {
            printLine(Util.capitalize(cmd) + " node \nUsage: node " + cmd + " <id> [options]\n");
            try { parser.printHelpOn(out); }
            catch (IOException ignore) {}

            printLine();
            handleGenericOptions(args, true);
            return;
        }

        if (args.isEmpty()) throw new Error("id required");
        String id = args.remove(0);

        OptionSet options;
        try { options = parser.parse(args.toArray(new String[args.size()])); }
        catch (OptionException e) {
            try { parser.printHelpOn(out); }
            catch (IOException ignore) {}

            printLine();
            throw new Error(e.getMessage());
        }

        String timeout = (String) options.valueOf("timeout");
        Boolean force = (Boolean) options.valueOf("force");

        HashMap<String, String> params = new HashMap<>();
        params.put("node", id);
        if (timeout != null) params.put("timeout", timeout);
        if (force != null) params.put("force", "" + force);

        JSONObject json;
        try { json = (JSONObject) sendRequest("/node/" + cmd, params); }
        catch (IOException e) { throw new Error("" + e); }

        String status = "" + json.get("status");
        @SuppressWarnings("unchecked") List<JSONObject> nodesJson = (List<JSONObject>) json.get("nodes");

        List<Node> nodes = new ArrayList<>();
        for (JSONObject nodeJson : nodesJson) nodes.add(new Node(nodeJson));

        String title = nodes.size() > 1 ? "nodes " : "node ";
        switch (status) {
            case "started":case "stopped": title += status + ":"; break;
            case "scheduled": title += status + "to " + cmd +  ":"; break;
            case "timeout":  throw new Error(cmd + " timeout");
        }

        printLine(title);
        for (Node node : nodes) {
            printNode(node, 1);
            printLine();
        }
    }

    private static void printNode(Node node, int indent) {
        printLine("id: " + node.id, indent);
        printLine("type: " + node.type.name().toLowerCase(), indent);
        printLine("state: " + node.state.name().toLowerCase(), indent);
        printLine("resources: " + nodeResources(node), indent);

        if (node.reservation != null) printLine("reservation: " + nodeReservation(node.reservation), indent);
        if (node.runtime != null) printNodeRuntime(node.runtime, indent);
    }

    private static void printNodeRuntime(Node.Runtime runtime, int indent) {
        printLine("runtime:", indent);
        printLine("task: " + runtime.taskId, indent + 1);
        printLine("executor: " + runtime.executorId, indent + 1);
        printLine("slave: " + runtime.slaveId, indent + 1);
    }

    private static void printCmds() {
        printLine("Commands:");
        printLine("list       - list nodes", 1);
        printLine("add        - add node", 1);
        printLine("update     - update node", 1);
        printLine("start      - start node", 1);
        printLine("stop       - stop node", 1);
    }

    private static String nodeResources(Node node) {
        String s = "";

        s += "cpus:" + node.cpus;
        s += ", mem:" + node.mem;

        return s;
    }

    private static String nodeReservation(Node.Reservation reservation) {
        String s = "";

        s += "cpus:" + reservation.cpus;
        s += ", mem:" + reservation.mem;

        return s;
    }
}
