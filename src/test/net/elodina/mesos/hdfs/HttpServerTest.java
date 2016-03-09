package net.elodina.mesos.hdfs;

import org.json.simple.JSONArray;
import org.json.simple.JSONAware;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class HttpServerTest extends MesosTestCase {
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
        Util.IO.delete(dir);

        super.after();
    }

    @Test
    public void download_jar() throws IOException {
        Scheduler.$.config.jar = new File(dir, "hdfs-mesos-0.1.jar");
        Util.IO.writeFile(Scheduler.$.config.jar, "jar");

        byte[] data = download("/jar/hdfs-mesos.jar");
        assertEquals("jar", new String(data));
    }

    @Test
    public void download_hadoop() throws IOException {
        Scheduler.$.config.hadoop = new File(dir, "hadoop-1.2.1.tar.gz");
        Util.IO.writeFile(Scheduler.$.config.hadoop, "hadoop");

        byte[] data = download("/hadoop/hadoop.tar.gz");
        assertEquals("hadoop", new String(data));
    }

    @Test
    public void handle_health() throws IOException {
        byte[] response = download("/health");
        assertEquals("ok\n", new String(response));
    }

    @Test
    public void api_node_list() throws IOException {
        // no nodes
        JSONArray json = (JSONArray) sendRequest("/node/list");
        List<Node> nodes = Node.fromJsonArray(json);
        assertTrue(nodes.isEmpty());

        // 2 nodes
        Node nn = Nodes.addNode(new Node("nn", Node.Type.NAMENODE));
        Node dn = Nodes.addNode(new Node("dn", Node.Type.DATANODE));

        json = (JSONArray) sendRequest("/node/list");
        nodes = Node.fromJsonArray(json);
        assertEquals(Arrays.asList(nn, dn), nodes);

        // single node
        json = (JSONArray) sendRequest("/node/list?node=nn");
        nodes = Node.fromJsonArray(json);
        assertEquals(Arrays.asList(nn), nodes);
    }

    @Test
    public void api_node_add_update() throws IOException {
        // add namenode
        JSONArray json = (JSONArray) sendRequest("/node/add?node=nn&type=namenode");
        assertEquals(1, Nodes.getNodes().size());

        Node nn = Nodes.getNode("nn");
        assertEquals(Node.Type.NAMENODE, nn.type);
        assertEquals(Arrays.asList(nn), Node.fromJsonArray(json));

        // add datanode
        json = (JSONArray) sendRequest("/node/add?node=dn&type=datanode");
        assertEquals(2, Nodes.getNodes().size());

        Node dn = Nodes.getNode("dn");
        assertEquals(Node.Type.DATANODE, dn.type);
        assertEquals(Arrays.asList(dn), Node.fromJsonArray(json));

        // update nodes
        json = (JSONArray) sendRequest("/node/update?node=*&mem=2048");
        assertEquals(Arrays.asList(nn, dn), Node.fromJsonArray(json));

        assertEquals(2048, nn.mem);
        assertEquals(2048, dn.mem);
    }

    public JSONAware sendRequest(String uri) throws IOException {
        return Cli.sendRequest(uri, Collections.<String, String>emptyMap());
    }

    public byte[] download(String uri) throws IOException {
        URL url = new URL(Cli.api + uri);
        HttpURLConnection c = (HttpURLConnection) url.openConnection();

        try {
            ByteArrayOutputStream data = new ByteArrayOutputStream();
            Util.IO.copyAndClose(c.getInputStream(), data);
            return data.toByteArray();
        } finally {
            c.disconnect();
        }
    }
}
