# Hadoop MapReduce 
## HomeWork 4 for CS6240 Fall 2018

Code author
-----------
`Shubham Rastogi`

Installation
------------
These components are installed:
- JDK 1.8
- Hadoop 2.9.1
- Maven
- AWS CLI (for EMR execution)

Environment
-----------
1) Example ~/.bash_aliases:<br>
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64<br>
export HADOOP_HOME=/home/shubham/tools/hadoop-2.9.1<br>
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop<br>
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin<br>

2) Explicitly set JAVA_HOME in $HADOOP_HOME/etc/hadoop/hadoop-env.sh:<br>
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64<br>

Execution
---------
All of the build & execution commands are organized in the Makefile.<br>
1) Unzip project file.
2) Open command prompt.
3) Navigate to directory where project files unzipped.
4) Add the input files (edges.csv, nodes.csv) inside the input folder under the project.
5) Edit the Makefile to customize the environment at the top.
	Sufficient for standalone: hadoop.root, jar.name, local.input, aws.bucket.name, aws.subtask.dir
	Other defaults acceptable for running standalone.
6) Standalone Hadoop:
	make switch-standalone		-- set standalone Hadoop environment (execute once)
	make local
7) Pseudo-Distributed Hadoop: (https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation)
	make switch-pseudo			-- set pseudo-clustered Hadoop environment (execute once)
	make pseudo					-- first execution
	make pseudoq				-- later executions since namenode and datanode already running 
8) AWS EMR Hadoop: (you must configure the emr.* config parameters at top of Makefile)
	make upload-input-aws		-- only before first execution
	make aws					-- check for successful execution with web interface (aws.amazon.com)
	download-output-aws			-- after successful execution & termination
