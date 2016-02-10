package net.elodina.mesos.hdfs;

import org.junit.Test;

import static org.junit.Assert.*;

public class NodeTest {
    @Test
    public void toJson_fromJson() {
        Node node = new Node("node");
        node.cpus = 2;
        node.mem = 1024;

        Node read = new Node();
        read.fromJson(node.toJson());

        assertEquals(node.cpus, read.cpus, 0.001);
        assertEquals(node.mem, read.mem);
    }
}
