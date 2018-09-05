#!/usr/bin/env bash

docker run -it --rm  \
    --name spark-shell  \
    --entrypoint spark-shell  \
    --network ewg-cluster  \
    -p 4040:4040  \
    -t bigdata/spark \
    --master spark://spark-master:7077