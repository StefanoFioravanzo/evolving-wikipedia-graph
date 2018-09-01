#!/usr/bin/env bash

CLASS=bigdata.wikiparser.EWG
MASTER=spark://spark-master:7077
TARGET=target/scala-2.11/evolving_wikipedia_graph-assembly-0.1.jar
ARGS=""

docker run -it --rm \
  --name spark-submit \
  --entrypoint spark-submit \
  --network sandbox-cluster \
  --volume "${PWD}/./":/project \
  -t bigdata/spark --class ${CLASS} --master ${MASTER} \
  /project/${TARGET} ${ARGS}