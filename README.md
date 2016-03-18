HDFS Mesos
==========
* [Intro](#intro)
* [Mesos in Vagrant](#mesos-in-vagrant)
* [Running Scheduler](#running-scheduler)
* [Running HDFS cluster](#running-hdfs-cluster)
* [Using CLI](#using-cli)
* [Using REST](#using-rest)
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


Using CLI
---------
Project provides cli with following structure:
```
# ./hdfs-mesos.sh help
Usage: <cmd> ...

Commands:
  help [cmd [cmd]] - print general or command-specific help
  scheduler        - start scheduler
  node             - node management
```

Help is provided for each command and sub-command:
```
# ./hdfs-mesos.sh help node
Node management commands
Usage: node <cmd>

Commands:
  list       - list nodes
  add        - add node
  update     - update node
  start      - start node
  stop       - stop node
  remove     - remove node

Run `help node <cmd>` to see details of specific command

# ./hdfs-mesos.sh help node add
Add node
Usage: node add <ids> [options]

Option (* = required)  Description
---------------------  -----------
--core-site-opts       Hadoop core-site.xml options.
--cpus <Double>        CPU amount (0.5, 1, 2).
--executor-jvm-opts    Executor JVM options.
--hadoop-jvm-opts      Hadoop JVM options.
--hdfs-site-opts       Hadoop hdfs-site.xml options.
--mem <Long>           Mem amount in Mb.
* --type               node type (name_node, data_node).

Generic Options
Option  Description
------  -----------
--api   REST api url (same as --api option for
          scheduler).
```

All node-related commands support bulk operations using node-id-expressions.
Examples:
```
# ./hdfs-mesos.sh node add dn0..1 --type=datanode
nodes added:
  id: dn0
  type: datanode
  ...

  id: dn1
  type: datanode
  ...

# ./hdfs-mesos.sh node update dn* --cpus=1
nodes updated:
  id: dn0
  ...
  resources: cpus:1.0, mem:512

  id: dn1
  ...
  resources: cpus:1.0, mem:512

# ./hdfs-mesos.sh node start dn0,dn1
nodes started:
  id: dn0
  ...

  id: dn0
  ...
```

Id expression examples:
- `nn`     - matches node with id nn
- `*`      - matches any node (should be slash-escaped in shell)
- `dn*`    - matches node with id starting with dn
- `dn0..2` - matches nodes dn0, dn1, dn2


Using REST
----------
Most CLI commands map to REST api call. Examples:
--------------------------------------------------------------------------------------------
| CLI                                        | REST                                        |
--------------------------------------------------------------------------------------------
|`node add nn --type=namenode --cpus=2`      |`/api/node/add?node=nn&type=namenode&cpus=2` |
--------------------------------------------------------------------------------------------

Having Issue
------------
Please read this README carefully, to make sure you problem is not already described.

Also make sure that your issue is not duplicating any existing one.

**DO NOT** post general question like "I am having problem with mesos"
to the issue list. Please use generic QA sites like http://stackoverflow.com
for that.

Issues list: https://github.com/elodina/hdfs-mesos/issues