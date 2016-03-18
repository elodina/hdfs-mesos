Mesos in Vagrant
================

- [Intro](#intro)
- [General Info](#general-info)
- [Host Names](#host-names)
- [Startup](#startup)
- [Configuration](#configuration)
- [Logs](#logs)

Intro
-----
This project provides Vagrant environment for running Mesos cluster.

You should be familiar with:
- Vagrant - https://www.vagrantup.com/docs/getting-started/
- Mesos   - http://mesos.apache.org/documentation/latest/

General Info
------------
Vagrantfile creates Mesos cluster with following nodes:
- master;
- slave0..slave(N-1) (N is specified in Vagrantfile);

Master node provides WEB UI listening on http://master:5050
Both master and slave nodes runs Mesos slave daemons.

Master node has pre-installed marathon scheduler.
Slave nodes may have pre-installed docker (uncomment in init.sh).

Host's public key, placed in `vagrant/.vagrant` dir, will be
copied to `authorized_hosts`, so direct access like `ssh vagrant@master|slaveX`
should work.

Nodes ssh keys are pre-generated and added to each node's `authorized_hosts`.
So internode `ssh` should work without password.

For general Mesos overview please refer to
http://mesos.apache.org/documentation/latest/mesos-architecture/

Host Names
----------
During first run `Vagrantfile` creates `hosts` file which
contains host names for cluster nodes. It is recommended
to append the content of its "cluster nodes" section to `/etc/hosts`
(or other OS-specific location) of the running (hosting) OS to be able to refer
master and slaves by names.

Startup
-------
Mesos master and slaves daemons are started automatically.

Each slave node runs 'mesos-slave' daemon while master runs both
'mesos-master' and 'mesos-slave' daemons.

Daemons could be controlled by using:
`/etc/init.d/mesos-{master|slave} {start|stop|status|restart}`

Configuration
-------------
Configuration is read from the following locations:
- `/etc/mesos`, `/etc/mesos-{master|slave}`
  for general or master|slave specific CLI options;
- `/etc/default/mesos`, `/etc/default/mesos-{master|slave}`
  for general or master|slave specific environment vars;

Please refer to CLI of 'mesos-master|slave' daemons and `/usr/bin/mesos-init-wrapper`
for details.

Logs
----
Logs are written to `/var/log/mesos/mesos-{master|slave}.*`

