package net.elodina.mesos.hdfs;

import net.elodina.mesos.util.IO;
import net.elodina.mesos.util.Request;
import org.json.simple.JSONArray;
import org.json.simple.JSONAware;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.fail;

public class HttpServerTest extends HdfsMesosTestCase {
    private HttpServer server;
    private File dir;

    @Before
    public void before() throws Exception {
        super.before();

        server = new HttpServer();
        server.start();

        dir = File.createTempFile(HttpServerTest.class.getSimpleName(), null);
        assertTrue(dir.delete());
        assertTrue(dir.mkdirs());
    }

    @After
    public void after() throws Exception {
        server.stop();
        IO.delete(dir);

        super.after();
    }

    @Test
    public void download_jar() throws IOException {
        Scheduler.$.config.jar = new File(dir, "hdfs-mesos-0.1.jar");
        IO.writeFile(Scheduler.$.config.jar, "jar");

        byte[] data = download("/jar/hdfs-mesos.jar");
        assertEquals("jar", new String(data));
    }

    @Test
    public void download_hadoop() throws IOException {
        Scheduler.$.config.hadoop = new File(dir, "hadoop-1.2.1.tar.gz");
        IO.writeFile(Scheduler.$.config.hadoop, "hadoop");

        byte[] data = download("/hadoop/hadoop.tar.gz");
        assertEquals("hadoop", new String(data));
    }

    @Test
    public void handle_health() throws IOException {
        byte[] response = download("/health");
        assertEquals("ok\n", new String(response));
    }

    @Test
    public void node_list() throws IOException {
        // no nodes
        JSONArray json = request("/node/list");
        List<Node> nodes = Node.fromJsonArray(json);
        assertTrue(nodes.isEmpty());

        // 2 nodes
        Node nn = Nodes.addNode(new Node("nn", Node.Type.NAMENODE));
        Node dn = Nodes.addNode(new Node("dn", Node.Type.DATANODE));

        json = request("/node/list");
        nodes = Node.fromJsonArray(json);
        assertEquals(Arrays.asList(nn, dn), nodes);

        // single node
        json = request("/node/list?node=nn");
        nodes = Node.fromJsonArray(json);
        assertEquals(Arrays.asList(nn), nodes);

        // invalid node
        try { request("/node/list?node=0..a"); fail(); }
        catch (IOException e) { assertTrue(e.getMessage(), e.getMessage().contains("invalid node")); }
    }

    @Test
    public void node_add_update() throws IOException {
        // add namenode
        JSONArray json = request("/node/add?node=nn&type=namenode");
        assertEquals(1, Nodes.getNodes().size());

        Node nn = Nodes.getNode("nn");
        assertEquals(Node.Type.NAMENODE, nn.type);
        assertEquals(Arrays.asList(nn), Node.fromJsonArray(json));

        // add datanode
        json = request("/node/add?node=dn&type=datanode");
        assertEquals(2, Nodes.getNodes().size());

        Node dn = Nodes.getNode("dn");
        assertEquals(Node.Type.DATANODE, dn.type);
        assertEquals(Arrays.asList(dn), Node.fromJsonArray(json));

        // update nodes
        json = request("/node/update?node=*&mem=2048");
        assertEquals(Arrays.asList(nn, dn), Node.fromJsonArray(json));

        assertEquals(2048, nn.mem);
        assertEquals(2048, dn.mem);
    }

    @Test
    public void node_add_update_node_validation() {
        // no node
        try { request("/node/add"); fail(); }
        catch (IOException e) { assertTrue(e.getMessage(), e.getMessage().contains("node required")); }

        // invalid node
        try { request("/node/add?node=0..a"); fail(); }
        catch (IOException e) { assertTrue(e.getMessage(), e.getMessage().contains("invalid node")); }

        // duplicate node
        Node dn = Nodes.addNode(new Node("dn", Node.Type.DATANODE));
        try { request("/node/add?node=dn"); fail(); }
        catch (IOException e) { assertTrue(e.getMessage(), e.getMessage().contains("duplicate node")); }

        // node not found
        try { request("/node/update?node=unknown"); fail(); }
        catch (IOException e) { assertTrue(e.getMessage(), e.getMessage().contains("node not found")); }

        // node not idle
        dn.state = Node.State.STARTING;
        try { request("/node/update?node=dn"); fail(); }
        catch (IOException e) { assertTrue(e.getMessage(), e.getMessage().contains("node not idle")); }
    }

    @Test
    public void node_add_update_type_validation() {
        // no type
        try { request("/node/add?node=a"); fail(); }
        catch (IOException e) { assertTrue(e.getMessage(), e.getMessage().contains("type required")); }

        // invalid type
        try { request("/node/add?node=a&type=abc"); fail(); }
        catch (IOException e) { assertTrue(e.getMessage(), e.getMessage().contains("invalid type")); }

        // duplicate namenode
        Nodes.addNode(new Node("nn", Node.Type.NAMENODE));
        try { request("/node/add?node=nn2&type=namenode"); fail(); }
        catch (IOException e) { assertTrue(e.getMessage(), e.getMessage().contains("duplicate namenode")); }
    }

    @Test
    public void node_add_update_other_validation() {
        Nodes.addNode(new Node("nn", Node.Type.NAMENODE));

        // cpus
        try { request("/node/update?node=nn&cpus=invalid"); fail(); }
        catch (IOException e) { assertTrue(e.getMessage(), e.getMessage().contains("invalid cpus")); }

        // mem
        try { request("/node/update?node=nn&mem=invalid"); fail(); }
        catch (IOException e) { assertTrue(e.getMessage(), e.getMessage().contains("invalid mem")); }

        // coreSiteOpts
        try { request("/node/update?node=nn&coreSiteOpts=invalid"); fail(); }
        catch (IOException e) { assertTrue(e.getMessage(), e.getMessage().contains("invalid coreSiteOpts")); }

        // hdfsSiteOpts
        try { request("/node/update?node=nn&hdfsSiteOpts=invalid"); fail(); }
        catch (IOException e) { assertTrue(e.getMessage(), e.getMessage().contains("invalid hdfsSiteOpts")); }
    }

    @Test
    public void node_start_stop() throws IOException {
        Node nn = Nodes.addNode(new Node("nn", Node.Type.NAMENODE));

        // schedule start
        JSONObject json = request("/node/start?node=nn&timeout=0");
        assertEquals("scheduled", "" + json.get("status"));
        assertEquals(Node.State.STARTING, nn.state);

        // schedule stop
        json = request("/node/stop?node=nn&timeout=0");
        assertEquals("scheduled", "" + json.get("status"));
        assertEquals(Node.State.STOPPING, nn.state);
    }

    @Test
    public void node_start_stop_validation() {
        // node required
        try { request("/node/start"); fail(); }
        catch (IOException e) { assertTrue(e.getMessage(), e.getMessage().contains("node required")); }

        // invalid node
        try { request("/node/start?node=0..a"); fail(); }
        catch (IOException e) { assertTrue(e.getMessage(), e.getMessage().contains("invalid node")); }

        // node not found
        try { request("/node/start?node=a"); fail(); }
        catch (IOException e) { assertTrue(e.getMessage(), e.getMessage().contains("node not found")); }

        // node not idle
        Node nn = Nodes.addNode(new Node("nn", Node.Type.NAMENODE));
        nn.state = Node.State.RUNNING;

        try { request("/node/start?node=nn"); fail(); }
        catch (IOException e) { assertTrue(e.getMessage(), e.getMessage().contains("node not idle")); }

        // node idle
        nn.state = Node.State.IDLE;
        try { request("/node/stop?node=nn"); fail(); }
        catch (IOException e) { assertTrue(e.getMessage(), e.getMessage().contains("node idle")); }

        // node external
        nn.externalFsUri = "uri";
        try { request("/node/start?node=nn"); fail(); }
        catch (IOException e) { assertTrue(e.getMessage(), e.getMessage().contains("node external")); }

        // timeout
        nn.externalFsUri = null;
        try { request("/node/start?node=nn&timeout=invalid"); fail(); }
        catch (IOException e) { assertTrue(e.getMessage(), e.getMessage().contains("invalid timeout")); }
    }

    @Test
    public void node_remove() throws IOException {
        Nodes.addNode(new Node("nn", Node.Type.NAMENODE));
        Nodes.addNode(new Node("dn", Node.Type.DATANODE));

        JSONArray json = request("/node/remove?node=dn");
        assertEquals(1, Nodes.getNodes().size());
        assertEquals(Arrays.asList("dn"), json);

        json = request("/node/remove?node=nn");
        assertTrue(Nodes.getNodes().isEmpty());
        assertEquals(Arrays.asList("nn"), json);
    }

    @Test
    public void node_remove_validation() {
        // node required
        try { request("/node/remove"); fail(); }
        catch (IOException e) { assertTrue(e.getMessage(), e.getMessage().contains("node required")); }

        // invalid node
        try { request("/node/remove?node=0..a"); fail(); }
        catch (IOException e) { assertTrue(e.getMessage(), e.getMessage().contains("invalid node")); }

        // node not found
        try { request("/node/remove?node=a"); fail(); }
        catch (IOException e) { assertTrue(e.getMessage(), e.getMessage().contains("node not found")); }

        try { request("/node/remove?node=a*"); fail(); }
        catch (IOException e) { assertTrue(e.getMessage(), e.getMessage().contains("node not found")); }

        // node not idle
        Node nn = Nodes.addNode(new Node("nn", Node.Type.NAMENODE));
        nn.state = Node.State.RUNNING;
        try { request("/node/remove?node=nn"); fail(); }
        catch (IOException e) { assertTrue(e.getMessage(), e.getMessage().contains("node not idle")); }
    }

    @SuppressWarnings("unchecked")
    public <T extends JSONAware> T request(String uri) throws IOException {
        Request.Response response = new Request(Cli.api + "/api" + uri).send();
        if (response.code() != 200) throw new IOException("Error " + response.code() + ": " + response.message());

        String json = response.asText();
        if (json == null) return null;

        try { return (T) new JSONParser().parse(json); }
        catch (ParseException e) { throw new IOException(e); }
    }

    public byte[] download(String uri) throws IOException {
        return new Request(Cli.api + uri).send().body();
    }
}
