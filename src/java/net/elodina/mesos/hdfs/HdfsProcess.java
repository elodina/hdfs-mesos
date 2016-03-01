package net.elodina.mesos.hdfs;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HdfsProcess {
    private Node node;
    private String hostname;

    private Process process;

    public HdfsProcess(Node node, String hostname) {
        this.node = node;
        this.hostname = hostname;
    }

    public void start() throws IOException, InterruptedException {
        createCoreSiteXml();
        createHdfsSiteXml();
        if (node.type == Node.Type.NAME_NODE) formatNameNode();
        process = startProcess();
    }

    public int waitFor() throws InterruptedException {
        if (process != null)
            return process.waitFor();

        throw new IllegalStateException("!started");
    }

    public void stop() {
        process.destroy();
    }

    private void createCoreSiteXml() throws IOException {
        String content =
            "<configuration>\n" +
            "<property>\n" +
            "  <name>hadoop.tmp.dir</name>\n" +
            "  <value>" + new File(Executor.dataDir, "tmp") + "</value>\n" +
            "</property>\n\n" +
            "<property>\n" +
            "  <name>fs.default.name</name>\n" +
            "  <value>" + node.runtime.fsUri + "</value>\n" +
            "</property>\n" +
            "</configuration>";

        File file = new File(Executor.hadoopDir, "conf/core-site.xml");
        Util.IO.writeFile(file, content);
    }

    private void createHdfsSiteXml() throws IOException {
        Map<String, String> props = new HashMap<>();

        if (node.type == Node.Type.NAME_NODE)
            props.put("dfs.http.address", hostname + ":" + node.reservation.ports.get(Node.Port.NAME_NODE_HTTP));
        else
            props.put("dfs.datanode.http.address", hostname + ":" + node.reservation.ports.get(Node.Port.DATA_NODE_HTTP));

        String content = "<configuration>\n";
        for (String name : props.keySet()) {
            content += "<property>\n" +
                       "  <name>" + name + "</name>\n" +
                       "  <value>" + props.get(name) + "</value>\n" +
                       "</property>\n";
        }
        content += "</configuration>";

        File file = new File(Executor.hadoopDir, "conf/hdfs-site.xml");
        Util.IO.writeFile(file, content);
    }

    private void formatNameNode() throws IOException, InterruptedException {
        ProcessBuilder builder = new ProcessBuilder(Executor.hadoop().getPath(), "namenode", "-format")
            .redirectOutput(ProcessBuilder.Redirect.INHERIT)
            .redirectError(ProcessBuilder.Redirect.INHERIT);

        builder.environment().put("JAVA_HOME", "" + Executor.javaHome);

        int code = builder.start().waitFor();
        if (code != 0) throw new IllegalStateException("Failed to format FS: process exited with " + code);
    }

    private Process startProcess() throws IOException {
        String cmd;
        switch (node.type) {
            case NAME_NODE: cmd = "namenode"; break;
            case DATA_NODE: cmd = "datanode"; break;
            default: throw new IllegalStateException("unsupported node type " + node.type);
        }

        ProcessBuilder builder = new ProcessBuilder(Executor.hadoop().getPath(), cmd)
            .redirectOutput(new File(node.type.name().toLowerCase() + ".out"))
            .redirectError(new File(node.type.name().toLowerCase() + ".err"));

        Map<String, String> env = builder.environment();
        env.put("JAVA_HOME", "" + Executor.javaHome);
        if (node.hadoopJvmOpts != null) env.put("HADOOP_OPTS", node.hadoopJvmOpts);

        return builder.start();
    }
}
