package net.elodina.mesos.hdfs;

import net.elodina.mesos.api.Master;
import net.elodina.mesos.api.Task;
import net.elodina.mesos.api.scheduler.SchedulerDriver;
import net.elodina.mesos.test.MesosTestCase;
import net.elodina.mesos.util.Net;
import org.apache.log4j.BasicConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

@Ignore
public class HdfsMesosTestCase extends MesosTestCase {
    TestSchedulerDriver schedulerDriver = new TestSchedulerDriver();

    @Before
    public void before() throws Exception {
        BasicConfigurator.configure();
        Scheduler.$.initLogging();

        File storageFile = Files.createTempFile(MesosTestCase.class.getSimpleName(), null).toFile();
        assertTrue(storageFile.delete());
        Nodes.storage = Storage.file(storageFile);
        Nodes.reset();

        Scheduler.Config config = Scheduler.$.config;
        config.api = "http://localhost:" + Net.findAvailPort();
        config.jar = new File("hdfs-mesos-0.1.jar");
        config.hadoop = new File("hadoop-1.2.1.tar.gz");

        Cli.api = config.api;
        Scheduler.$.subscribed(schedulerDriver, "id", new Master());
    }

    @After
    public void after() throws Exception {
        Scheduler.$.disconnected();
        BasicConfigurator.resetConfiguration();

        Scheduler.Config config = Scheduler.$.config;
        config.api = null;
        config.jar = null;

        Nodes.storage.clear();
    }

    public static class TestSchedulerDriver implements SchedulerDriver {
        public List<String> declinedOffers = new ArrayList<>();
        public List<String> acceptedOffers = new ArrayList<>();

        public List<Task> launchedTasks = new ArrayList<>();
        public List<String> killedTasks = new ArrayList<>();
        public List<String> reconciledTasks = new ArrayList<>();

        public boolean stopped;
        public List<Message> sentFrameworkMessages = new ArrayList<>();

        @Override
        public void declineOffer(String id) { declinedOffers.add(id); }

        @Override
        public void launchTask(String offerId, Task task) {
            acceptedOffers.add(offerId);
            launchedTasks.add(task);
        }

        @Override
        public void reconcileTasks(List<String> ids) {
            if (ids.isEmpty()) reconciledTasks.add("");
            reconciledTasks.addAll(ids);
        }

        @Override
        public void killTask(String id) { killedTasks.add(id); }

        @Override
        public boolean run() { throw new UnsupportedOperationException(); }

        @Override
        public void stop() { stopped = true; }

        public static class Message {
            public String executorId;
            public String slaveId;
            public byte[] data;

            public Message(String executorId, String slaveId, byte[] data) {
                this.executorId = executorId;
                this.slaveId = slaveId;
                this.data = data;
            }
        }
    }
}
