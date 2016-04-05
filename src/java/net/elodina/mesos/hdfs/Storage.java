package net.elodina.mesos.hdfs;

import net.elodina.mesos.util.IO;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.nio.charset.Charset;

public abstract class Storage {
    public static Storage file(File file) { return new FileStorage(file); }
    public static Storage zk(String zk) { return new ZkStorage(zk); }

    public static Storage byUri(String uri) {
        // zk:master:2181/hdfs-mesos, file:hdfs-mesos.json
        int colonIdx = uri.indexOf(":");
        if (colonIdx == -1) throw new IllegalArgumentException(uri);

        String protocol = uri.substring(0, colonIdx);
        String value = uri.substring(colonIdx + 1);
        switch (protocol) {
            case "file": return new FileStorage(new File(value));
            case "zk": return new ZkStorage(value);
            default: throw new IllegalArgumentException(uri);
        }
    }

    public abstract void save();
    public abstract void load();
    public abstract void clear();

    private static class FileStorage extends Storage {
        private File file;
        private FileStorage(File file) { this.file = file; }

        @Override
        public void save() {
            JSONObject json = Nodes.toJson();
            try { IO.writeFile(file, "" + json); }
            catch (IOException e) { throw new IOError(e); }
        }

        @Override
        public void load() {
            if (!file.exists()) return;

            JSONObject obj;
            try { obj = (JSONObject) new JSONParser().parse(IO.readFile(file)); }
            catch (ParseException | IOException e) { throw new IOError(e); }

            Nodes.fromJson(obj);
        }

        @Override
        public void clear() {
            if (!file.exists()) return;
            if (!file.delete()) throw new IOError(new IOException("failed to delete " + file));
        }
    }

    private static class ZkStorage extends Storage {
        private String connect, path;

        private ZkStorage(String zk) {
            // master:2181/hdfs-mesos,  master:2181,master2:2181/hdfs-mesos
            int slashIdx = zk.indexOf("/");
            if (slashIdx == -1) throw new IllegalArgumentException(zk);

            connect = zk.substring(0, slashIdx);
            path = zk.substring(slashIdx);
        }

        private ZkClient client() { return new ZkClient(connect, 30000, 30000, new BytesPushThroughSerializer()); }

        @Override
        public void save() {
            ZkClient client = client();
            try {
                client.createPersistent(path, true);
                client.writeData(path, Nodes.toJson().toString().getBytes(Charset.forName("utf-8")));
            } finally { client.close(); }
        }

        @Override
        public void load() {
            ZkClient client = client();
            try {
                byte[] bytes = client.readData(path, true);
                if (bytes == null) return;

                JSONObject json = (JSONObject) new JSONParser().parse(new String(bytes, Charset.forName("utf-8")));
                Nodes.fromJson(json);
            } catch (ParseException e) {
                throw new IOError(e);
            } finally {
                client.close();
            }
        }

        @Override
        public void clear() {
            ZkClient client = client();
            try { client.delete(path); }
            finally { client.close(); }
        }
    }
}


