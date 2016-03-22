package net.elodina.mesos.hdfs;

import net.elodina.mesos.test.MesosTestCase;
import net.elodina.mesos.util.Net;
import org.apache.log4j.BasicConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

import java.io.File;
import java.nio.file.Files;

import static org.junit.Assert.assertTrue;

@Ignore
public class HdfsMesosTestCase extends MesosTestCase {
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
        Scheduler.$.registered(schedulerDriver, frameworkId(), master());
    }

    @After
    public void after() throws Exception {
        Scheduler.$.disconnected(schedulerDriver);
        BasicConfigurator.resetConfiguration();

        Scheduler.Config config = Scheduler.$.config;
        config.api = null;
        config.jar = null;

        Nodes.storage.clear();
    }
}
