HDFS Mesos
==========

Intro
-----
This project allows running HDFS on Mesos.

You should be familiar with HDFS and Mesos basics:
- [http://mesos.apache.org/documentation/latest/]
- [https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html]

For issues see: [Having Issue]

Building
--------
Clone and build the project
```
# git clone https://github.com/elodina/hdfs-mesos.git
# cd hdfs-mesos
# ./gradlew jar
# wget https://archive.apache.org/dist/kafka/0.8.2.2/kafka_2.10-0.8.2.2.tgz
```

Running in Vagrant
-------------------
Project includes vagrant environment, that allows to run it locally.

Download hadoop tarball first:
```
# cd hdfs-mesos
# wget https://archive.apache.org/dist/hadoop/core/hadoop-1.2.1/hadoop-1.2.1.tar.gz
```

Start vagrant env:
```
# cd vagrant
# vagrant up
```
It creates mesos master and slave nodes. Please see [vagrant/README.md] for more details.
Please add node names to `/etc/hosts` as described [vagrant/README.md#host-names]


Having Issue
------------
Please read this README carefully, to make sure you problem is not already described.

Also make sure that your issue is not duplicating any existing one.

DO NOT post general question like "I am having problem with mesos"
to the issue list. Please use generic QA sites like http://stackoverflow.com
for that.

Issues list: https://github.com/elodina/hdfs-mesos/issues