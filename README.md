# MaGDAA Rhizome Analaysis 1.0 #
The purpose of this application is to provide a way to generate some statistics about the contents of [Rhizome][rhizome] databases that have been created by the [Serval Mesh][serval-mesh] software. 

## Compiling the Software ##

To compile the software use the included build.xml file with the [Apache Ant][apache-ant] build tool. The following libraries must be stored in the libs directory:

 1. [Apache Commons CLI][commons-cli] (version 1.2) 
 2. [Apache Commons Configuration][commons-config] (version 1.9)
 3. [Apache Commons IO][commons-io] (version 2.4)
 4. [Apache Commons Lang][commons-lang] (version 2.6)
 5. [Apache Commons Logging][commons-log] (version 1.1.2)
 6. [MySQL Connector][mysql-connector] (5.1.23)
 7. [opencsv][opencsv] (2.3)
 8. [sqlite-jdbc][sqlite-jdbc] (3.7.2)

## Using the Software ##

To use the software, first compile it using the instructions above. Then create a MySQL database and a user with full permissions on that database. Finally to run the software us the following command:

`java -jar dist/RhizomeAnalysis.jar {command line options}`

Executing the application without command line options results in the following text being displayed:

<pre>
MaGDAA Rhizome Analysis - 1.0
Error in parsing arguments:
Missing required options: task, properties, table
usage: java -jar RhizomeAnalysis.jar
 -dataset <path>      path to the parent directory of a dataset
 -input <path>        path to a single input rhizome database
 -output <path>       path to an output file
 -properties <path>   path to the properties file
 -table <string>      name of table to work with
 -tablet <string>     id of the tablet
 -task <string>       task to undertake
</pre>

The command line options are explained more fully in sections below.

### -dataset ###

The `-dataset` command line option specifies a path to the parent directory of a dataset. For example:

`-dataset /full-path/to-a/parent-dir`

In the context of this application a dataset is a directory structure where each directory underneath the parent directory represents a single device, in each directory is stored the Rhizome database from that device. For example:
<pre>
parent-dir/magdaa-01/rhizome.db
parent-dir/magdaa-02/rhizome.db
parent-dir/magdaa-15/rhizome.db
parent-dir/laptop/rhizome.db
</pre>

### -input ###
The `-input` command line option specifies the path to a single rhizome database. For example: 

`-input /full-path/to-a/rhizome.db`

### -output ###
The `-output` command line option specifies the path to a single output file. For example:

`-output /full-path/for-a/output-file.csv`

### -properties ###
The `-properties` command line option specifies the path to a properties file. This file contains the connection details for the MySQL database that will store the aggregate data. For example:

`-properties /full-path/to-a/default.properties`

A sample properties file looks likes this:

<pre>
# host name of the database server
db.host = localhost
# username used to connect to the database sever
db.user = magdaa
# password used to connect to the database server
db.password = magdaa123
# name of the database on the server
db.database = magdaa
</pre>

### -table ###

The `-table` command line option specifies which table containing the aggregate data will be used for the given task. A table represents a single deployment of the Serval Mesh software which you wish to analyse. For example if the deployment occurred on the 2013-03-10 you may want to use a table name this like this:

`-table 2013_03_10`

### -tablet ###

The `-tablet` command one option specifies the identifier for a device on the mesh network. For the purposes of the MaGDAA Project these were predominantly tablets. For example:

`-tablet magdaa-01`

### -task ###

The `-task` command line option specifies the task that is to be undertaken. For example:

`-task create-table`
A full list of tasks is provided in the next section. 

## Analysis Tasks ##

The following tasks can be undertaken by the software. 

### create-table ###

The `create-table` task undertakes the creation of a table, and associated indexes, in the MySQL database to store the aggregated data from the Rhizome databases. The following command line options are required for this task:

1. `-task create-table`
2. `-properties`
3. `-table`

### import-data ###

The `import-data` task imports data from a single Rhizome database into the specified table in the MySQL database. The following command line options are required for this task:

1. `-task import-data`
2. `-properties`
3. `-table`
4. `-input`

### batch-import ###

The `batch-import` task imports data from a set of Rhizome databases into the specified table in the MySQL database. The following command line options are required for this task:

1. `-task batch-import`
2. `-properties`
3. `-table`
4. `-dataset`

### update-origin ###

The `update-origin` task adjusts a field in the MySQL table to identify a particular file was created and initially added to Rhizome on a specified device. The following command line options are required for this task:

1. `-task update-origin`
2. `-properties`
3. `-table`
4. `-dataset`

It is assumed that the path specified in the `-dataset` command line option is in the same format as specified earlier, except that it contains the actual files added by the device to Rhizome. 

### statistics ###

The `statistics` task will output in the terminal the results of some statistical analysis. 

The following command line options are required for this task:

1. `-task statistics`
2. `-properties`
3. `-table`

The statistical analysis looks like this:
<pre>
MaGDAA Rhizome Analysis - 1.0

Statistical Analysis for table: 2013_03_01
Total unique files on the mesh: 249
Total bundles on the mesh: 3584
Total unique data size on the mesh: 183 KB
Total data size (including duplicates) on the mesh: 2 MB
Average file size: 753 bytes
Average number of bundles per device: 162.9091
Total number of files without resilient copies: 36
Total number of files with resilient copies: 213
Maximum resilient copy count: 21
Minimum resilient copy count: 6
Approximate Maximum time delay before first resilient copy: 1:41:10 (H:m:s)
Approximate Minimum time delay before first resilient copy: 0:0:7 (H:m:s)
Total number of files not on the laptop: 36
</pre>

Editing the source code for the StatisticalAnalysis class can be undertaken to achieve different results as required by an individual deployment.

### chart-bundles-over-time ###

The `chart-bundles-over-time` task creates a CSV file which can be used to form the basis of a chart which shows the distribution of files over time during a deployment of the Rhizome and Serval Mesh technology. 

The following command line options are required for this task:

1. `-task chart-bundles-over-time`
2. `-properties`
3. `-table`
4. `-output`

A sample application for charting the results of this command using [R][r-project] is available in the tools directory.








[rhizome]: http://developer.servalproject.org/dokuwiki/doku.php?id=content:technologies:rhizome
[serval-mesh]: http://developer.servalproject.org/dokuwiki/doku.php?id=content:technologies:servalmesh
[apache-ant]: http://ant.apache.org/
[commons-cli]: http://commons.apache.org/proper/commons-cli/
[commons-config]:http://commons.apache.org/proper/commons-configuration/
[commons-io]: http://commons.apache.org/proper/commons-io/
[commons-lang]: http://commons.apache.org/proper/commons-lang/
[commons-log]: http://commons.apache.org/proper/commons-logging/
[mysql-connector]: http://dev.mysql.com/downloads/connector/j/
[opencsv]: http://opencsv.sourceforge.net/
[sqlite-jdbc]: https://bitbucket.org/xerial/sqlite-jdbc
[r-project]: http://www.r-project.org/