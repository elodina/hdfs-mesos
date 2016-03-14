package net.elodina.mesos.hdfs;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CliTest extends MesosTestCase {
    private ByteArrayOutputStream out;
    private HttpServer server;

    @Before
    public void before() throws Exception {
        super.before();

        out = new ByteArrayOutputStream();
        Cli.out = new PrintStream(out, true);

        server = new HttpServer();
        server.start();
    }

    @After
    public void after() throws Exception {
        Cli.out = System.out;
        server.stop();
        super.after();
    }

    @Test
    public void node_list() {
        // no nodes
        exec("node list");
        assertOutContains("no nodes");

        // 2 nodes
        Nodes.addNode(new Node("nn", Node.Type.NAMENODE));
        Nodes.addNode(new Node("dn", Node.Type.DATANODE));
        exec("node list");
        assertOutContains("nodes:");
        assertOutContains("id: nn");
        assertOutContains("id: dn");

        // single node
        exec("node list nn");
        assertOutContains("node:");
        assertOutContains("id: nn");
        assertOutNotContains("id: dn");

        // expr error
        try { exec("node list 0..a"); fail(); }
        catch (Cli.Error e) { assertTrue(e.getMessage(), e.getMessage().contains("invalid node")); }
    }

    private void exec(String cmd) {
        out.reset();
        Cli.handle(new ArrayList<>(Arrays.asList(cmd.split(" "))));
    }

    private void assertOutContains(String message) {
        if (!out.toString().contains(message))
            throw new AssertionError("out expected contain \"" + message + "\", actual:\n" + out.toString());
    }

    private void assertOutNotContains(String message) {
        if (out.toString().contains(message))
            throw new AssertionError("out not expected contain \"" + message + "\", actual:\n" + out.toString());
    }
}
