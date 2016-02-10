package net.elodina.mesos.hdfs;

import org.json.simple.JSONObject;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class NodesTest {
    @Before
    public void before() {
        Nodes.$.reset();
    }

    @Test
    public void getNodes() {
        Node n0 = Nodes.$.addNode(new Node("n0"));
        Node n1 = Nodes.$.addNode(new Node("n1"));
        Node n2 = Nodes.$.addNode(new Node("n2"));
        assertEquals(Arrays.asList(n0, n1, n2), Nodes.$.getNodes());
    }

    @Test
    public void getNode() {
        assertNull(Nodes.$.getNode("n0"));
        Node n0 = Nodes.$.addNode(new Node("n0"));
        assertSame(n0, Nodes.$.getNode("n0"));
    }

    @Test
    public void addNode() {
        Node n0 = Nodes.$.addNode(new Node("n0"));
        assertEquals(Arrays.asList(n0), Nodes.$.getNodes());

        try { Nodes.$.addNode(new Node("n0")); fail(); }
        catch (IllegalArgumentException e) { assertTrue(e.getMessage(), e.getMessage().contains("duplicate")); }
    }

    @Test
    public void removeNode() {
        Node n0 = Nodes.$.addNode(new Node("n0"));
        Node n1 = Nodes.$.addNode(new Node("n1"));
        Node n2 = Nodes.$.addNode(new Node("n2"));
        assertEquals(Arrays.asList(n0, n1, n2), Nodes.$.getNodes());

        Nodes.$.removeNode(n1);
        assertEquals(Arrays.asList(n0, n2), Nodes.$.getNodes());

        Nodes.$.removeNode(n1);
    }

    @Test
    public void toJson_fromJson() {
        Nodes.$.frameworkId = "id";
        Node n0 = Nodes.$.addNode(new Node("n0"));
        Node n1 = Nodes.$.addNode(new Node("n1"));

        JSONObject json = Nodes.$.toJson();
        Nodes.$.fromJson(json);

        assertEquals("id", Nodes.$.frameworkId);
        assertEquals(Arrays.asList(n0, n1), Nodes.$.getNodes());
    }
}
