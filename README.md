## Evolving Wikipedia Graph


This project was developed for the BigDataAndSocialNetworks course @UniversityOfTrento during my master degree in Computer Science.

The assignment required to implement a distributed parser of Wikipedia revision files and produce an evolving graph. The graph nodes are the article pages and the edges are the links between them, with the temporal information about how many references were present in the page at that time.

More specifically, the final graph structure is as follows:

List of edges Article A:  

[2001-12-12T06:32:58,2002-04-03T17:10:18] PageB 2  
[2001-12-12T06:32:58,2002-04-03T17:16:35] PageC 3  
[2001-12-12T06:32:58,2002-09-04T09:03:13] PageD 1  
[2002-09-04T09:03:13,2003-06-05T17:27:30] PageE 1  
[2002-04-03T17:16:35,2003-06-05T17:27:30] PageB 1  
...  

So every article has a reference to the pages it is linked to, and every link is valid in a temporal range with its reference count. As the page is revisioned, its link counts may change, as can be seen by the link to PageB.

--

#### Technology

TODO: Explain here how I used Hadoop & Spark

#### Project Structure

*[ Have a look at the documentation of each function for a better understanding of what the code does ]*

Under `src/main/java/bigdata/input/` directory can be found the custom Hadoop distributed readers:

- `WikipediaInputFormat.java`: Custom distributed Hadoop reader to stream in wikipedia history files and parse revisions to return a single revision with its article information to a map job
- `XMLInputFormat.java`: More general Hadoop reader to stream XML files and return the specified tag blocks (like a single block `<page></page>`)

Under `src/main/scala/bigdata/wikiparser/` can be found all the spark application classes:

- `EWG.java`: main application entrypoint
- `Link.scala`: Helper class to store a Link and its information (timestamp, count)
- `PageParser.scala`: Collection of spark functions to parse a page and its links
- `LinkParser.scala`: Helper functions to parse the links and the counts of the links from the text of a page/revision
- `RevisionParser.scala`: Collection of spark function to parse revisions and write to HDFS the resulting graph nodes/edges

#### Build and Run

The project relies on the SBT build system, all the dependencies are defined in `build.sbt`. To compile and run the project run the following commands from the root of the project:

TODOL show how to run and compile with out using fat jars

```bash
sbt compile
sbt run
```

To just test and debug the application you can run run Spark in local mode, so that the Spark and Hadoop environment will be virtualized on the local machine.

Environment configuration can be set in `src/main/resources/application.conf` by settings the `env` parameter either to `local` or `dist` (to be used with the docker environment running).

**Workflow**

A quick reference over the application workflow to parse wikipedia revision files when running:

`EWG.parseRevisions()` [ Start revision parse job ] -> `RevisionParser.readWikiRevisionsFromDump()` [ Init Hadoop reader to stream in revision file ] -> `RevisionParser.parseRevisions()` [ Map job to parse the links in article text ] -> `RevisionParser.mapPagesToInternalLinksWthIndex()` -> `RevisionParser.combineLinks()`[ Combine all the revisions of the same article page ] -> `RevisionParser.sortAndPrintOut()` [ Final parsing and writing to HDFS ] 

#### Docker

To setup an Hadoop HDFS + Spark cluster on our laptop and simulate a real cluster settings, docker compose files are provided.

To run a Hadoop cluster composed of one Hadoop NameNode and one Hadoop DataNode run:

```bash
docker-compose -f docker-compose-hadoop.yml
```

Running with the default configuration docker will map the folders `hadoop-dfs-name` and `hadoop-dfs-data` to persist hdfs data between run. You can also explore the HDFS dashboard navigating to `localhost:50070`.

To setup a Spark cluster, instead, run:

```bash
docker-compose -f docker-compose-spark-cluster.yml up --scale spark-worker=2
```

You can set whatever number of workers you prefer with the `--scale` argument. Current the compose file supports a maximum of 10 worker due to port allocation range. If you want to spin up > 10 workers, increase the port range mapping in the docker-compose file.

The spark master container will map the spark web ui dashboard to `localhost:7077`, while the workers dashboards will be mapped form port `8081` and increasing.