HDFS Mesos
==========

Intro
-----
This project allows running HDFS on Mesos.

You should be familiar with HDFS and Mesos basics:
- http://mesos.apache.org/documentation/latest/
- https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html

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

\1. Download hadoop tarball first:
```
# cd hdfs-mesos
# wget https://archive.apache.org/dist/hadoop/core/hadoop-1.2.1/hadoop-1.2.1.tar.gz
```

2. Start vagrant env:
```
# cd vagrant
# vagrant up
```
It creates mesos master and slave nodes.

\3. Add vagrant node names ([vagrant/README.md#host-names](vagrant/README.md#host-names)) to `/etc/hosts`

\4. Start scheduler:
```
# cd ..
# ./hdfs-mesos.sh scheduler --api=http://$host_ip:7000 --master=zk://master:2181/mesos --user=vagrant
```
where $host_ip is host ip address accessible from vagrant nodes.

Now scheduler should be running and you can proceed with starting hdfs nodes.

Note: if Scheduler is not receiving offers it could be required to specify following:
```
# export LIBPROCESS_IP=$host_ip
```

Please see [vagrant/README.md](vagrant/README.md) for more details.

Having Issue
------------
Please read this README carefully, to make sure you problem is not already described.

Also make sure that your issue is not duplicating any existing one.

DO NOT post general question like "I am having problem with mesos"
to the issue list. Please use generic QA sites like http://stackoverflow.com
for that.

Issues list: https://github.com/elodina/hdfs-mesos/issues