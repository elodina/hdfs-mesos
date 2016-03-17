HDFS Mesos
==========

This project allows running HDFS on Mesos.

You should be familiar with HDFS and Mesos basics:
- http://mesos.apache.org/documentation/latest/
- https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html

Content:
* [Mesos in Vagrant](#mesos-in-vagrant)
* [Running Scheduler](#running-scheduler)
* [Having Issue](#having-issue)


Mesos in Vagrant
----------------
Project includes [vagrant environment](/vagrant), that allows to run Mesos cluster locally.

If you are going to use external Mesos cluster, you can skip this section.

**1.** Start vagrant nodes:
```
# cd hdfs-mesos/vagrant
# vagrant up
```
It creates mesos master and slave nodes.

**2.**  Add vagrant node names ([vagrant/README.md#host-names](vagrant/README.md#host-names)) to `/etc/hosts`

Now Mesos in vagrant should be running. You can proceed with starting scheduler.

------------

Note: if running Scheduler is not receiving offers it could be required to specify LIBPROCESS_IP:
```
# export LIBPROCESS_IP=$host_ip
```
For more details about vagrant environment please read [vagrant/README.md](vagrant/README.md)


Running Scheduler
-----------------
**1.** Download `hdfs-mesos\*.jar` OR clone & build the project:

Download jar:
```
# mkdir hdfs-mesos
# cd hdfs-mesos
# wget https://github.com/elodina/hdfs-mesos/releases/download/0.0.1.0/hdfs-mesos-0.0.1.0.jar
```

*OR* clone & build:
```
# git clone https://github.com/elodina/hdfs-mesos.git
# cd hdfs-mesos
# ./gradlew jar
# wget https://archive.apache.org/dist/kafka/0.8.2.2/kafka_2.10-0.8.2.2.tgz
```

**2.** Download hadoop tarball:
```
# wget https://archive.apache.org/dist/hadoop/core/hadoop-1.2.1/hadoop-1.2.1.tar.gz
```

**3.** Start scheduler:
```
# ./hdfs-mesos.sh scheduler --api=http://$scheduler:7000 --master=zk://$master:2181/mesos --user=vagrant
```
where:
- `$scheduler` is scheduler address accessible from slave nodes;
- `$master` master address accessible from scheduler node;

Now scheduler should be running and you can proceed with starting HDFS nodes.

Having Issue
------------
Please read this README carefully, to make sure you problem is not already described.

Also make sure that your issue is not duplicating any existing one.

**DO NOT** post general question like "I am having problem with mesos"
to the issue list. Please use generic QA sites like http://stackoverflow.com
for that.

Issues list: https://github.com/elodina/hdfs-mesos/issues