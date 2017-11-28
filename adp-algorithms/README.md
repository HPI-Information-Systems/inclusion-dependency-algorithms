Advanced Data Profiling Seminar
===============================

## Building

Build requirement is only Java 1.8. The build system is Gradle.
In order to build all algorithm JARs, invoke:

    ./gradlew assemble

You will not be required to install Gradle - upon first invocation an appropriate distribution will
be downloaded and run automatically.  
You will find the algorithm JARs inside the `<algorithm>/build/libs` directory.

## Algorithms

Algorithm implementations (and possibly companion projects) should reside in their own subdirectory.
Do not forget to add those to the list of known submodules inside the [`settings.gradle`](settings.gradle).


## Debugging

Building metanome-cli:

1. Start by installing [Metanome](https://github.com/HPI-Information-Systems/Metanome) to your local Maven repository.
2. Then build [Metacrate](https://github.com/stratosphere/metadata-ms]). At the time of
  you will be requiring the changes from [this PR](https://github.com/stratosphere/metadata-ms/pull/61)
  in order to build against the latest version of Metanome (master).
3. Finally follow the build instructions of [metanome-cli](https://github.com/sekruse/metanome-cli).

The following files represent debug shell scripts which can be used to attach a debugger to a running instance
of the algorithm using metanome-cli. All bold represents
parameters specific to the algorithm / machine and therefore possibly require modification.

This one feeds `test.csv`:

<pre>
./gradlew assemble
java \
-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=<b>5005</b> \
-cp metanome-cli-1.1.1-SNAPSHOT.jar:<b>spider/build/libs/spider-0.1.0-SNAPSHOT-file.jar</b> \
de.metanome.cli.App \
--algorithm <b>de.metanome.algorithms.spider.SpiderFileAlgorithm</b> \
--file-key <b>TABLE</b> \
--files <b>test.csv</b> \
--algorithm-config <b>TEMPORARY_FOLDER_PATH:/my/temp</b> \
--output print \
--header
</pre>

And this one looks up table `person` inside a MySQL database:

<pre>
./gradlew assemble
java \
-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=<b>5005</b> \
-cp metanome-cli-1.1.1-SNAPSHOT.jar:<b>spider/build/libs/spider-0.1.0-SNAPSHOT-database.jar</b> \
de.metanome.cli.App \
--algorithm <b>de.metanome.algorithms.spider.SpiderDatabaseAlgorithm</b> \
--db-type <b>mysql</b> \
--db-connection <b>pgpass.txt</b> \
--table-key <b>TABLE</b> \
--tables <b>person</b> \
--algorithm-config <b>TEMPORARY_FOLDER_PATH:/my/temp</b> \
--output print
</pre>

`pgpass.txt` contains more details about the database connection and credentials in the format
 `hostname:port:database:username:password` (see the
 [docs](https://wiki.postgresql.org/wiki/Pgpass)).