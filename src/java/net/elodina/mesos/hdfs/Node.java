package net.elodina.mesos.hdfs;

import org.json.simple.JSONObject;

public class Node {
    public String id;
    public State state = State.IDLE;

    public double cpus = 0.5;
    public long mem = 512;

    public Node() {}

    public Node(String id) {
        this.id = id;
    }

    @SuppressWarnings("unchecked")
    public JSONObject toJson() {
        JSONObject json = new JSONObject();

        json.put("id", id);
        json.put("state", "" + state.name().toLowerCase());

        json.put("cpus", cpus);
        json.put("mem", mem);

        return json;
    }

    public void fromJson(JSONObject json) {
        id = (String) json.get("id");
        state = State.valueOf(json.get("state").toString().toUpperCase());

        cpus = ((Number) json.get("cpus")).doubleValue();
        mem = ((Number) json.get("mem")).longValue();
    }

    public int hashCode() { return id.hashCode(); }

    public boolean equals(Object obj) { return obj instanceof Node && ((Node) obj).id.equals(id); }

    public String toString() { return id; }

    public static enum State {
        IDLE,
        STARTING,
        RUNNING,
        STOPPING
    }
}
