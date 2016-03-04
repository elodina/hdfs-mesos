package net.elodina.mesos.hdfs;

import org.json.simple.JSONObject;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class NodesTest {
    @Before
    public void before() {
        Nodes.reset();
    }

    @Test
    public void getNodes() {
        Node n0 = Nodes.addNode(new Node("n0", Node.Type.NAMENODE));
        Node n1 = Nodes.addNode(new Node("n1", Node.Type.DATANODE));
        Node n2 = Nodes.addNode(new Node("n2", Node.Type.DATANODE));
        assertEquals(Arrays.asList(n0, n1, n2), Nodes.getNodes());
    }

    @Test
    public void getNodes_by_type() {
        Node nn = Nodes.addNode(new Node("nn", Node.Type.NAMENODE));
        Node dn0 = Nodes.addNode(new Node("dn0", Node.Type.DATANODE));
        Node dn1 = Nodes.addNode(new Node("dn1", Node.Type.DATANODE));

        assertEquals(Arrays.asList(nn), Nodes.getNodes(Node.Type.NAMENODE));
        assertEquals(Arrays.asList(dn0, dn1), Nodes.getNodes(Node.Type.DATANODE));
    }

    @Test
    public void getNodes_by_state() {
        Node n0 = Nodes.addNode(new Node("n0"));
        Node n1 = Nodes.addNode(new Node("n1", Node.Type.DATANODE));
        Node n2 = Nodes.addNode(new Node("n2", Node.Type.DATANODE));

        n1.state = Node.State.RUNNING;
        n2.state = Node.State.RUNNING;

        assertEquals(Arrays.asList(n0), Nodes.getNodes(Node.State.IDLE));
        assertEquals(Arrays.asList(n1, n2), Nodes.getNodes(Node.State.RUNNING));
    }

    @Test
    public void getNode() {
        assertNull(Nodes.getNode("n0"));
        Node n0 = Nodes.addNode(new Node("n0"));
        assertSame(n0, Nodes.getNode("n0"));
    }

    @Test
    public void expandExpr() {
        Nodes.addNode(new Node("nn", Node.Type.NAMENODE));
        Nodes.addNode(new Node("dn0", Node.Type.DATANODE));
        Nodes.addNode(new Node("dn1", Node.Type.DATANODE));

        // id list
        assertEquals(Arrays.asList("nn", "dn2"), Nodes.expandExpr("nn,dn2"));

        // wildcard
        assertEquals(Arrays.asList("dn0", "dn1"), Nodes.expandExpr("dn*"));
        assertEquals(Arrays.asList("nn", "dn0", "dn1"), Nodes.expandExpr("*"));

        // range
        assertEquals(Arrays.asList("1", "2", "3"), Nodes.expandExpr("1..3"));
        assertEquals(Arrays.asList("dn1", "dn2", "dn3"), Nodes.expandExpr("dn1..3"));
    }

    @Test
    public void addNode() {
        Node nn = Nodes.addNode(new Node("nn", Node.Type.NAMENODE));
        assertEquals(Arrays.asList(nn), Nodes.getNodes());

        // duplicate id
        try { Nodes.addNode(new Node("nn", Node.Type.DATANODE)); fail(); }
        catch (IllegalArgumentException e) { assertTrue(e.getMessage(), e.getMessage().contains("duplicate")); }

        // second namenode
        try { Nodes.addNode(new Node("nn1", Node.Type.NAMENODE)); fail(); }
        catch (IllegalArgumentException e) { assertTrue(e.getMessage(), e.getMessage().contains("second")); }
    }

    @Test
    public void removeNode() {
        Node n0 = Nodes.addNode(new Node("n0", Node.Type.NAMENODE));
        Node n1 = Nodes.addNode(new Node("n1", Node.Type.DATANODE));
        Node n2 = Nodes.addNode(new Node("n2", Node.Type.DATANODE));
        assertEquals(Arrays.asList(n0, n1, n2), Nodes.getNodes());

        Nodes.removeNode(n1);
        assertEquals(Arrays.asList(n0, n2), Nodes.getNodes());

        Nodes.removeNode(n1);
    }

    @Test
    public void toJson_fromJson() {
        Nodes.frameworkId = "id";
        Node n0 = Nodes.addNode(new Node("n0"));
        Node n1 = Nodes.addNode(new Node("n1", Node.Type.DATANODE));

        JSONObject json = Nodes.toJson();
        Nodes.fromJson(json);

        assertEquals("id", Nodes.frameworkId);
        assertEquals(Arrays.asList(n0, n1), Nodes.getNodes());
    }
}
