package net.elodina.mesos.hdfs;

import net.elodina.mesos.util.Strings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.*;

public class CliTest extends HdfsMesosTestCase {
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

    @Test
    public void node_add_update() {
        // add node
        exec("node add nn --type=namenode --cpus=2 --mem=1024");
        assertOutContains("node added");

        Node nn = Nodes.getNode("nn");
        assertNotNull(nn);
        assertEquals(2, nn.cpus, 0.001);
        assertEquals(1024, nn.mem);

        // update node
        exec("node update nn --core-site-opts=a=1");
        assertOutContains("node updated");
        assertEquals(Strings.parseMap("a=1"), nn.coreSiteOpts);
    }

    @Test
    public void node_remove() {
        Nodes.addNode(new Node("nn", Node.Type.NAMENODE));
        Nodes.addNode(new Node("dn", Node.Type.DATANODE));

        // remove dn
        exec("node remove dn");
        assertOutContains("node dn removed");
        assertEquals(1, Nodes.getNodes().size());
        assertNull(Nodes.getNode("dn"));

        // remove nn
        exec("node remove nn");
        assertOutContains("node nn removed");
        assertTrue(Nodes.getNodes().isEmpty());

        // no nodes to remove
        try { exec("node remove *"); fail(); }
        catch (Cli.Error e) { assertTrue(e.getMessage(), e.getMessage().contains("node not found")); }
    }

    @Test
    public void node_start_stop() {
        // start node
        Node nn = Nodes.addNode(new Node("nn", Node.Type.NAMENODE));
        exec("node start nn --timeout=0");
        assertOutContains("node scheduled to start:");
        assertOutContains("id: nn");
        assertEquals(Node.State.STARTING, nn.state);

        // stop node
        exec("node stop nn --timeout=0");
        assertOutContains("node scheduled to stop:");
        assertOutContains("id: nn");
        assertEquals(Node.State.STOPPING, nn.state);
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
