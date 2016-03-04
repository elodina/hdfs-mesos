package net.elodina.mesos.hdfs;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HdfsProcess {
    private static Logger logger = Logger.getLogger(HdfsProcess.class);

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

        if (node.type == Node.Type.NAMENODE && !isNameNodeFormatted())
            formatNameNode();

        process = startProcess();
    }

    public int waitFor() throws InterruptedException {
        if (process == null) throw new IllegalStateException("!started");

        int code = process.waitFor();
        logger.info("Process finished with code " + code);

        return code;
    }

    public void stop() {
        logger.info("Stopping process");
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

        if (node.type == Node.Type.NAMENODE) {
            props.put("dfs.http.address", hostname + ":" + node.reservation.ports.get(Node.Port.HTTP));
            props.put("dfs.name.dir", "" + getNameNodeDir());
        } else {
            props.put("dfs.datanode.http.address", hostname + ":" + node.reservation.ports.get(Node.Port.HTTP));
            props.put("dfs.datanode.address", hostname + ":" + node.reservation.ports.get(Node.Port.DATA));
            props.put("dfs.datanode.ipc.address", hostname + ":" + node.reservation.ports.get(Node.Port.IPC));
            props.put("dfs.data.dir", "" + getDataNodeDir());
        }

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

    private File getNameNodeDir() { return new File(Executor.dataDir, "namenode"); }
    private boolean isNameNodeFormatted() { return new File(getNameNodeDir(), "current").isDirectory(); }

    private File getDataNodeDir() { return new File(Executor.dataDir, "datanode"); }

    private void formatNameNode() throws IOException, InterruptedException {
        logger.info("Formatting namenode");

        ProcessBuilder builder = new ProcessBuilder(Executor.hadoop().getPath(), "namenode", "-format")
            .redirectOutput(ProcessBuilder.Redirect.INHERIT)
            .redirectError(ProcessBuilder.Redirect.INHERIT);

        builder.environment().put("JAVA_HOME", "" + Executor.javaHome);

        int code = builder.start().waitFor();
        if (code != 0) throw new IllegalStateException("Failed to format namenode: process exited with " + code);
    }

    private Process startProcess() throws IOException {
        String cmd;
        switch (node.type) {
            case NAMENODE: cmd = "namenode"; break;
            case DATANODE: cmd = "datanode"; break;
            default: throw new IllegalStateException("unsupported node type " + node.type);
        }

        ProcessBuilder builder = new ProcessBuilder(Executor.hadoop().getPath(), cmd)
            .redirectOutput(new File(node.type.name().toLowerCase() + ".out"))
            .redirectError(new File(node.type.name().toLowerCase() + ".err"));

        Map<String, String> env = builder.environment();
        env.put("JAVA_HOME", "" + Executor.javaHome);
        if (node.hadoopJvmOpts != null) env.put("HADOOP_OPTS", node.hadoopJvmOpts);

        logger.info("Starting process '" + Util.join(builder.command(), " ") + "'");
        return builder.start();
    }
}
