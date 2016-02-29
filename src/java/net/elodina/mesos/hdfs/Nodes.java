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

    public static Node getNode(String id) {
        for (Node node : nodes)
            if (node.id.equals(id)) return node;

        return null;
    }

    public static Node addNode(Node node) {
        if (getNode(node.id) != null) throw new IllegalArgumentException("duplicate node");

        if (node.type == Node.Type.NAME_NODE && !getNodes(Node.Type.NAME_NODE).isEmpty())
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
        if (json.containsKey("nodes")) {
            JSONArray nodesJson = (JSONArray) json.get("nodes");
            for (JSONObject nodeJson : (List<JSONObject>)nodesJson)
                nodes.add(new Node(nodeJson));
        }
    }
}
