#!/bin/bash
if [ ! -d "/opt/hadoop/data/nameNode/current" ]; then
    echo "Formatting NameNode..."
    hdfs namenode -format -force
fi

# Start the NameNode in the background
echo "Starting NameNode..."
hdfs namenode &

# Wait for the NameNode to start
sleep 10

# Upload the entire directory to HDFS
HDFS_TARGET_DIR="/data"
LOCAL_SOURCE_DIR="/finbench_datasets"

echo "Uploading directory $LOCAL_SOURCE_DIR to HDFS directory $HDFS_TARGET_DIR..."
hdfs dfs -mkdir -p $HDFS_TARGET_DIR
hdfs dfs -put -f "$LOCAL_SOURCE_DIR"/* $HDFS_TARGET_DIR/

# Keep the script running to ensure the container stays active
echo "HDFS setup completed. Keeping the container active..."
tail -f /dev/null
