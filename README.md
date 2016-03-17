HDFS Mesos
==========
* [Intro](#intro)
* [Mesos in Vagrant](#mesos-in-vagrant)
* [Running Scheduler](#running-scheduler)
* [Running HDFS cluster](#running-hdfs-cluster)
* [Having Issue](#having-issue)


Intro
-----
This project allows running HDFS on Mesos.

You should be familiar with HDFS and Mesos basics:
- http://mesos.apache.org/documentation/latest/
- https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html

Project requires:
- Mesos 0.23.0+
- JDK 1.7.x
- Hadoop 1.2.x


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


Running HDFS Cluster
---------------------
Project provides CLI & REST API for managing HDFS nodes. We will focus first on CLI.

**1.** Add namenode & datanode:
```
# ./hdfs-mesos.sh node add nn --type=namenode
node added:
  id: nn
  type: namenode
  state: idle
  resources: cpus:0.5, mem:512

# ./hdfs-mesos.sh node add dn0 --type=datanode
node added:
  id: dn0
  type: datanode
  state: idle
  resources: cpus:0.5, mem:512
```

**2.** Start nodes:
```
# ./hdfs-mesos.sh node start \*
nodes started:
  id: nn
  type: namenode
  state: running
  resources: cpus:0.5, mem:512
  reservation: cpus:0.5, mem:512, ports:http=5000,ipc=5001
  runtime:
    task: 383aaab9-982b-400e-aa35-463e66cdcb3b
    executor: 19065e07-a006-49a4-8f2b-636d8b1f2ad6
    slave: 241be3a2-39bc-417c-a967-82b4018a0762-S0 (master)

  id: dn0
  type: datanode
  state: running
  resources: cpus:0.5, mem:512
  reservation: cpus:0.5, mem:512, ports:http=5002,ipc=5003,data=5004
  runtime:
    task: 37f3bcbb-10a5-4323-96d2-aef8846aa281
    executor: 088463c9-5f2e-4d1d-8195-56427168b86f
    slave: 241be3a2-39bc-417c-a967-82b4018a0762-S0 (master)
```

Nodes are up & running now.

Note: starting may take some time. You can view the progress via Mesos UI.

**3.** Do some FS operations:
```
# hadoop fs -mkdir hdfs://master:5001/dir
# hadoop fs -ls hdfs://master:5001/
Found 1 items
drwxr-xr-x   - vagrant supergroup          0 2016-03-17 12:46 /dir
```
Note: namenode host and ipc port is used in fs url.


Having Issue
------------
Please read this README carefully, to make sure you problem is not already described.

Also make sure that your issue is not duplicating any existing one.

**DO NOT** post general question like "I am having problem with mesos"
to the issue list. Please use generic QA sites like http://stackoverflow.com
for that.

Issues list: https://github.com/elodina/hdfs-mesos/issues