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

RUN find / -name "*commons-cli*.jar" -delete

RUN wget https://repo1.maven.org/maven2/commons-cli/commons-cli/1.4/commons-cli-1.4.jar -P /opt/flink/lib/

# Copy Hadoop JARs
RUN mkdir -p /opt/flink/lib/hadoop && \
    cp -a $HADOOP_HOME/share/hadoop/. /opt/flink/lib/hadoop/

ENV FLINK_ENV_JAVA_OPTS="-Djava.class.path=/opt/flink/lib/commons-cli-1.4.jar:/opt/flink/lib/*:/opt/flink/lib/hadoop/*"