#!/usr/bin/env bash

docker run -it --rm \
    --name hdfs-shell \
    --volume "`pwd`":/project \
    --network sandbox-cluster \
    -e "CORE_CONF_fs_defaultFS=hdfs://hadoop-namenode:8020" \
    -e "CLUSTER_NAME=hadoop-sandbox" \
    -t uhopper/hadoop:latest \
    /bin/bash