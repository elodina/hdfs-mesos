package net.elodina.mesos.hdfs;

import java.io.File;
import java.io.IOException;

public class NNProcess {
    private String hostname;

    private Process process;

    public NNProcess(String hostname) {
        this.hostname = hostname;
    }

    public void start() throws IOException, InterruptedException {
        createCoreSiteXml();
        formatFs();
        process = startNN();
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
            "  <value>hdfs://" + hostname + ":54310</value>\n" +
            "</property>\n" +
            "</configuration>";

        File file = new File(Executor.hadoopDir, "conf/core-site.xml");
        Util.IO.writeFile(file, content);
    }

    private void formatFs() throws IOException, InterruptedException {
        ProcessBuilder builder = new ProcessBuilder(Executor.hadoop().getPath(), "namenode", "-format")
            .redirectOutput(ProcessBuilder.Redirect.INHERIT)
            .redirectError(ProcessBuilder.Redirect.INHERIT);

        builder.environment().put("JAVA_HOME", "" + Executor.javaHome);

        int code = builder.start().waitFor();
        if (code != 0) throw new IllegalStateException("Failed to format FS: process exited with " + code);
    }

    private Process startNN() throws IOException {
        ProcessBuilder builder = new ProcessBuilder(Executor.hadoop().getPath(), "namenode")
            .redirectOutput(new File("nn.out"))
            .redirectError(new File("nn.err"));

        builder.environment().put("JAVA_HOME", "" + Executor.javaHome);
        return builder.start();
    }
}
