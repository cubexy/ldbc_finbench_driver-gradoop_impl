FROM flink:1.9.3-scala_2.12

# Install dependencies
RUN apt-get update && apt-get install -y wget

RUN mkdir -p /hadoop

# Download and install Hadoop 2.8.5
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-2.8.5/hadoop-2.8.5.tar.gz && \
    tar -xzf hadoop-2.8.5.tar.gz -C /hadoop/ && \
    rm hadoop-2.8.5.tar.gz

# Set Hadoop environment variables
ENV HADOOP_HOME=/hadoop/hadoop-2.8.5
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV HADOOP_CLASSPATH=/hadoop/hadoop-2.8.5/etc/hadoop:/hadoop/hadoop-2.8.5/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/yarn/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/*:$HADOOP_HOME/contrib/capacity-scheduler/*.jar
ENV PATH=$PATH:$HADOOP_HOME/bin