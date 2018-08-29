#!/usr/bin/env bash

# remove all previous data
rm -rf "./output_data/"
mkdir output_data

# copy latest data from hdfs
docker run -it --rm \
    --name hdfs-shell \
    --volume "`pwd`"/output_data:/output_data \
    --network sandbox-cluster \
    -e "CORE_CONF_fs_defaultFS=hdfs://hadoop-namenode:8020" \
    -e "CLUSTER_NAME=hadoop-sandbox" \
    -t uhopper/hadoop:latest \
    hdfs dfs -get /ewg /output_data