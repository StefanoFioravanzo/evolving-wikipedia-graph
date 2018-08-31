## Evolving Wikipedia Graph


This project was developed for the BigDataAndSocialNetworks course @UniversityOfTrento during my master degree in Computer Science.

The assignment required to implement a distributed parser of Wikipedia revision files and produce an evolving graph. The graph nodes are the article pages and the edges are the links between them, with the temporal information about how many references were present in the page at that time.

More specifically, the final graph structure should be as follows:

List of edges Article A:  

[2001-12-12T06:32:58,2002-04-03T17:10:18] PageB 2  
[2001-12-12T06:32:58,2002-04-03T17:16:35] PageC 3  
[2001-12-12T06:32:58,2002-09-04T09:03:13] PageD 1  
[2002-09-04T09:03:13,2003-06-05T17:27:30] PageE 1  
[2002-04-03T17:16:35,2003-06-05T17:27:30] PageB 1  
...  

So every article has a reference to the pages it is linked to, and every link is valid in a temporal range with its reference count. As the page is revisioned, its link counts may change, as can be seen by the link to PageB.

--

### Data

Wikipedia offers free copies of all available content to interested users, which can be used for mirroring, personal use, offline use or database queries. The English Wikipedia dumps in SQL and XML can all be found [here] (dumps.wikimedia.org/enwiki/).

Under the `latest/` folder can be found the latest dump of Wikipedia. Inside this folder the archives are organized as follows:

- `pages-articles.xml`: Contains the current version of all article pages, templates, and other pages. These dumps are recommended for republishing content.
- `pages-meta-current.xml`:  Contains current version of all pages, including discussion and user "home" pages.
- `pages-meta-history.xml`: Contains complete text of every revision of every page. 
There are also stub dumps for each category that contain header information only for pages and revisions. For this project we will be using these files.

Dumps are compressed using `bz2` format, which uses a block-oriented compression format. This is pretty useful for the dump process as the process can easily recover from failures and can check completion status just by looking at the end blocks of a file. The `bz2` file format lents itself nicely to streaming jobs as it can be decompressed in blocks. We can exploit this features in our Hadoop input reader to stream in the compressed file without the need to decompress it first.

To have some perspective over the sheer size of the uncompressed data: the XML file containing current pages only was 53GiB, while the full 202 history dumps is about 10TiB.

For the purpose of this project we are interested in parsing the history dumps, which contain all the revisions of all articles in history. These file are named with the following pattern: `enwiki-latest-pages-meta-history%d%d.xml-pxxxxxpxxxxx.bz2`

The first number indicates the batch. There are currently 27 batches, and for each batch there are approximately 17 partitions each one of about 4GiB of compressed data, which can decompress even to 80-100 GiB.

### Technology

In order to parse and transform this huge data set, we make use of Hadoop and Spark distributed capabilities. First of all, we need some way to efficiently stream in an history revision file (preferably in compressed format) and serially parse it to produce on the fly small unit of computations - one revision - that can be passed to a `map` job.

Hadoop `InputFormat` is the component which manages how input files are read and split and defines a `RecordReader`, which is responsible to read actual records from the file. For our purposes we extended these base classes in a custom `WikipediaInputFormat` and `WikipediaRecordReader` to efficiently parse the wikipedia file XML structure and give only relevant information to the mapper tasks. 

The mapper job accessing the data through the WikipediaInputFormat will call the method `nextKeyValue` implemented in the `WikipediaInputFormat` class every time is has enough resources to process a new data pair.

To implement all the map and reduce operations to efficiently parse the wikipedia revisions we use Spark, which exposes a simple interface. Scala also helps a lot in writing functional code, being itself a functional first programming language.

All of the Spark transformations are in `LinkParser.scala` and `RevisionParser.scala`.

### Sample data

Under `data/` can be found some small data samples resembling the structure of a real wikipedia history file, used to test and debug the application logic.

`pages_history_samples` contains a few articles with some revisions, taken from the first history file, both in xml and in bz2 compressed format.

`articles1` is an extract from the first "articles" wikipedia dump - the first 2Mbytes - used for testing.

### Project Structure

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

### Build and Run

The project relies on the SBT build system, all the dependencies are defined in `build.sbt`. To compile and run the project run the following commands from the root of the project:

**TODO: show how to run and compile with out using fat jars**

```bash
sbt compile
sbt run
```

To just test and debug the application you can run run Spark in local mode, so that the Spark and Hadoop environment will be virtualized on the local machine.

Environment configuration can be set in `src/main/resources/application.conf` by setting the `env` parameter either to `local` or `dist` (to be used with the docker environment running).

To deploy the application to a spark cluster, we need to crate a fat jar with all the needed dependencies packaged inside the jar file. To do this we rely on an sbt plugin `sbt-assembly`, which is added to the `project/plugins.sbt` file. To create the fat jar run:

```
sbt assembly
```

The command will create a fat jar under `target/` directory.

**Workflow**

To have a better understanding of the parsing pipeline the application goes throw, here is the cascade of function calls from the application entrypoint, to the HDFS final write:

`EWG.parseRevisions()` [ Start revision parse job ] -> `RevisionParser.readWikiRevisionsFromDump()` [ Init Hadoop reader to stream in revision file ] -> `RevisionParser.parseRevisions()` [ Map job to parse the links in article text ] -> `RevisionParser.mapPagesToInternalLinksWthIndex()` -> `RevisionParser.combineLinks()`[ Combine all the revisions of the same article page ] -> `RevisionParser.sortAndPrintOut()` [ Final parsing and writing to HDFS ] 

### Docker

Docker compose files are provided to setup an Hadoop HDFS + Spark cluster on your laptop and simulate a real cluster settings.

**Hadoop HDFS**

To run a Hadoop cluster composed of one Hadoop NameNode and one Hadoop DataNode run:

```bash
docker-compose -f docker-compose-hadoop.yml
```

Running with the default configuration docker will map the folders `hadoop-dfs-name` and `hadoop-dfs-data` to persist hdfs data between run. You can also explore the HDFS dashboard navigating to `localhost:50070`.

To interact with the HDFS file system you can launch a container connected to the HDFS cluster by running the bash script `./launch_hdfs_shell.sh`. From there you can interact with the HDFS file system, for example:

```bash
# list all file at the root
hdfs dfs -ls /
# load file form local file system to hdfs
hdfs dsf -put <localsrc> <dst>
# show file content
hdfs dfs -cat <file_path>
```

For more commands refer to the [official guide] (https://hadoop.apache.org/docs/r2.4.1/hadoop-project-dist/hadoop-common/FileSystemShell.html).

**Spark Cluster**

To setup a Spark cluster made up of one spark master container and `n` spark worker containers run:

```bash
docker-compose -f docker-compose-spark-cluster.yml up --scale spark-worker=2
```

You can set whatever number of workers you prefer with the `--scale` argument. Currently the compose file supports a maximum of 10 workers due to port forwarding allocation range. If you want to spin up more than 10 workers, increase the port range mapping in the docker-compose file.

The spark master container will map the spark web ui dashboard to `localhost:7077`, while the workers dashboards will be mapped form port `8081` and increasing.

#### Deploy with spark-submit.sh

In order to deploy a spark job to a spark cluster we need to use `spark-submit.sh`, a spark provided script that will take care of properly setting up the environment for the application to run.

After creating an application far jar as explained before, we can deploy it to our docker cluster using the `launch_spark_submit.sh` script located at the root of the project. It is important to set the correct application class entry-point and the path to the fat jar at the top of the script.

#### Output data

Once the job has finished running, all the files will be saved in HDFS under the folder `/ewg`, one file per article. To download the output data to local file system for better handling, just run `download_output_data.sh`, which will save all the files under HDFS folder `/ewg` in the local folder `output_data/`.