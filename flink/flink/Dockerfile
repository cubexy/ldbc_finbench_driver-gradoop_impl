FROM flink:1.9.3-scala_2.12

# Install dependencies
RUN apt-get update && apt-get install -y wget

# Download and install Hadoop 2.8.5
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-2.8.5/hadoop-2.8.5.tar.gz && \
    tar -xzf hadoop-2.8.5.tar.gz -C /opt/ && \
    rm hadoop-2.8.5.tar.gz

# Set Hadoop environment variables
ENV HADOOP_HOME=/opt/hadoop-2.8.5
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV HADOOP_CLASSPATH=$HADOOP_HOME/etc/hadoop:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*
ENV PATH=$PATH:$HADOOP_HOME/bin

# Remove all commons-cli jars first
RUN find / -name "*commons-cli*.jar" -delete

# Copy Hadoop JARs
RUN mkdir -p /opt/flink/lib/hadoop && \
    cp -a $HADOOP_HOME/share/hadoop/. /opt/flink/lib/hadoop/