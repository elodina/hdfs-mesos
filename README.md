HDFS Mesos
==========

Intro
-----
This project allows running HDFS on Mesos.

You should be familiar with HDFS and Mesos basics:
- http://mesos.apache.org/documentation/latest/
- https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html

For issues see: [Having Issue](#having-issue)

Running Scheduler
-----------------
**1.** Download `hdfs-mesos\*.jar` OR clone & build project:

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
# cd hdfs-mesos
# wget https://archive.apache.org/dist/hadoop/core/hadoop-1.2.1/hadoop-1.2.1.tar.gz
```

**3.** Start scheduler:
```
# cd ..
# ./hdfs-mesos.sh scheduler --api=http://$host_ip:7000 --master=zk://master:2181/mesos --user=vagrant
```
where `$host_ip` is host ip address accessible from vagrant nodes.

Now scheduler should be running and you can proceed with starting HDFS nodes.


Using Vagrant
-------------
Project includes vagrant environment, that allows to run Mesos cluster locally.

Start vagrant env:
```
# cd hdfs-mesos/vagrant
# vagrant up
```
It creates mesos master and slave nodes.

Add vagrant node names ([vagrant/README.md#host-names](vagrant/README.md#host-names)) to `/etc/hosts`

-------------
Note: For more details please read [vagrant/README.md](vagrant/README.md)

Note: if running Scheduler is not receiving offers it could be required to specify LIBPROCESS_IP:
```
# export LIBPROCESS_IP=$host_ip
```

Having Issue
------------
Please read this README carefully, to make sure you problem is not already described.

Also make sure that your issue is not duplicating any existing one.

**DO NOT** post general question like "I am having problem with mesos"
to the issue list. Please use generic QA sites like http://stackoverflow.com
for that.

Issues list: https://github.com/elodina/hdfs-mesos/issues