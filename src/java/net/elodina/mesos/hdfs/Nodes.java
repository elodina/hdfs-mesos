package net.elodina.mesos.hdfs;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Nodes {
    public static Nodes $ = new Nodes();

    public Storage storage = Storage.file(new File("hdfs-mesos.json"));

    public String frameworkId;
    private List<Node> nodes = new ArrayList<>();

    private Nodes() {}

    public List<Node> getNodes() { return Collections.unmodifiableList(nodes); }

    public Node getNode(String id) {
        for (Node node : nodes)
            if (node.id.equals(id)) return node;

        return null;
    }

    public Node addNode(Node node) {
        if (getNode(node.id) != null) throw new IllegalArgumentException("duplicate node");

        nodes.add(node);
        return node;
    }

    public void removeNode(Node node) {
        nodes.remove(node);
    }

    public void reset() {
        frameworkId = null;
        nodes.clear();
    }

    public void save() { storage.save(); }
    public void load() { storage.load(); }

    @SuppressWarnings("unchecked")
    public JSONObject toJson() {
        JSONObject json = new JSONObject();

        if (frameworkId != null) json.put("frameworkId", frameworkId);

        JSONArray nodesJson = new JSONArray();
        for (Node node : nodes) nodesJson.add(node.toJson());
        if (!nodesJson.isEmpty()) json.put("nodes", nodesJson);

        return json;
    }

    @SuppressWarnings({"RedundantCast", "unchecked"})
    public void fromJson(JSONObject json) {
        if (json.containsKey("frameworkId")) frameworkId = (String) json.get("frameworkId");

        nodes.clear();
        if (json.containsKey("nodes")) {
            JSONArray nodesJson = (JSONArray) json.get("nodes");
            for (JSONObject nodeJson : (List<JSONObject>)nodesJson) {
                Node node = new Node();
                node.fromJson(nodeJson);
                nodes.add(node);
            }
        }
    }
}
