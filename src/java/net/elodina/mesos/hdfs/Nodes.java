package net.elodina.mesos.hdfs;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Nodes {
    public static Storage storage = Storage.file(new File("hdfs-mesos.json"));

    public static String frameworkId;
    private static List<Node> nodes = new ArrayList<>();

    private Nodes() {}

    public static List<Node> getNodes() { return Collections.unmodifiableList(nodes); }

    public static List<Node> getNodes(Node.State state) {
        List<Node> nodes = new ArrayList<>();
        for (Node node : getNodes()) if (node.state == state) nodes.add(node);
        return nodes;
    }

    public static List<Node> getNodes(Node.Type type) {
        List<Node> nodes = new ArrayList<>();
        for (Node node : getNodes()) if (node.type == type) nodes.add(node);
        return nodes;
    }

    public static List<Node> getNodes(List<String> ids) {
        List<Node> nodes = new ArrayList<>();

        for (String id : ids) {
            Node node = getNode(id);
            if (node != null) nodes.add(node);
        }

        return nodes;
    }

    public static Node getNode(String id) {
        for (Node node : nodes)
            if (node.id.equals(id)) return node;

        return null;
    }

    /*
        Expands expr. Examples:
        - nn, dn0, dn3                  -> nn, dn0, dn3
        - dn* (dn0, dn1, dn2 exists)    -> dn0, dn1, dn2
        - 0..3                          -> 0, 1, 2, 3
        - dn1..3                        -> dn1, dn2, dn3
     */
    public static List<String> expandExpr(String expr) {
        List<String> ids = new ArrayList<>();

        for (String part : expr.split(",")) {
            part = part.trim();

            if (part.endsWith("*")) ids.addAll(expandWildcard(expr));
            else if (part.contains("..")) ids.addAll(expandRange(part));
            else ids.add(part);
        }

        return ids;
    }

    private static List<String> expandWildcard(String expr) {
        List<String> ids = new ArrayList<>();

        String prefix = expr.substring(0, expr.length() - 1);
        for (Node node : getNodes())
            if (node.id.startsWith(prefix)) ids.add(node.id);

        return ids;
    }

    private static List<String> expandRange(String expr) {
        // dn0..5
        int rangeIdx = expr.indexOf("..");

        int startIdx = rangeIdx - 1;
        //noinspection StatementWithEmptyBody
        for (char[] chars = expr.toCharArray(); startIdx >= 0 && Character.isDigit(chars[startIdx]); startIdx--);
        startIdx ++;

        String prefix = expr.substring(0, startIdx);
        int start, end;
        try {
            start = Integer.parseInt(expr.substring(startIdx, rangeIdx));
            end = Integer.parseInt(expr.substring(rangeIdx + 2));
        } catch (NumberFormatException e) { throw new IllegalArgumentException(expr); }

        List<String> ids = new ArrayList<>();
        for (int i = start; i <= end; i++)
            ids.add(prefix + i);

        return ids;
    }

    public static Node addNode(Node node) {
        if (getNode(node.id) != null) throw new IllegalArgumentException("duplicate node");

        if (node.type == Node.Type.NAMENODE && !getNodes(Node.Type.NAMENODE).isEmpty())
            throw new IllegalArgumentException("second name node is not supported");

        nodes.add(node);
        return node;
    }

    public static void removeNode(Node node) {
        nodes.remove(node);
    }

    public static void reset() {
        frameworkId = null;
        nodes.clear();
    }

    public static void save() { storage.save(); }
    public static void load() { storage.load(); }

    @SuppressWarnings("unchecked")
    public static JSONObject toJson() {
        JSONObject json = new JSONObject();

        if (frameworkId != null) json.put("frameworkId", frameworkId);

        JSONArray nodesJson = new JSONArray();
        for (Node node : nodes) nodesJson.add(node.toJson());
        if (!nodesJson.isEmpty()) json.put("nodes", nodesJson);

        return json;
    }

    @SuppressWarnings({"RedundantCast", "unchecked"})
    public static void fromJson(JSONObject json) {
        if (json.containsKey("frameworkId")) frameworkId = (String) json.get("frameworkId");

        nodes.clear();
        if (json.containsKey("nodes"))
            nodes.addAll(Node.fromJsonArray((JSONArray) json.get("nodes")));
    }
}
