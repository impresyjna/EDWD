INSTALLING HADOOP:
(instructions from the lecture in more copyable format)

0. Install required packeges:
sudo apt-get install ssh rsync

1. Download Hadoop from:
http://hadoop.apache.org/releases.html

2. Untar the Hadoop:
tar xvfz hadoop-XXX.tar.gz

3. Set JAVA_HOME environment variable in 'etc/hadoop/hadoop-env.sh'.

4. Create an RSA key to be used by Hadoop when ssh'ing to localhost:
ssh-keygen -t dsa -P '' -f ~/.ssh/id_dsa 
cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys

5. Set HADOOP_PREFIX environment variable to Hadoop root directory.

6. Do changes to the configuration files:

in 'etc/hadoop/core-site.xml':

<configuration>
	<property>
		<name>fs.defaultFS</name>
		<value>hdfs://localhost:9000</value>
	</property>
</configuration>

in 'etc/hadoop/hdfs-site.xml':

<configuration>
	<property>
		<name>dfs.replication</name>
		<value>1</value>
	</property>
</configuration>

7. Format the Hadoop file system:
bin/hdfs namenode -format

8. Start NameNode daemon and DataNode daemon:
sbin/start-dfs.sh

9. Browse the web interface for the NameNode:
http://localhost:50070/
(check if there is 1 LiveNode)


RUNNING THE PROJECT:

1. Set 'hadoop.home', 'job.name' and 'job.data' in 'project.properties'.

2. Execute 'run' target.
