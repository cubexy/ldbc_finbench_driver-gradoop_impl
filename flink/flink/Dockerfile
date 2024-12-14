FROM flink:1.9.3-scala_2.12

# Install Hadoop dependencies
RUN apt-get update && apt-get install -y wget

# Download and install Hadoop
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-3.2.1/hadoop-3.2.1.tar.gz && \
    tar -xzf hadoop-3.2.1.tar.gz -C /opt/ && \
    rm hadoop-3.2.1.tar.gz

# Set Hadoop environment variables
ENV HADOOP_HOME=/opt/hadoop-3.2.1
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV HADOOP_CLASSPATH=$HADOOP_HOME/etc/hadoop:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*
ENV PATH=$PATH:$HADOOP_HOME/bin

# Remove all commons-cli jars first
RUN find / -name "*commons-cli*.jar" -delete

# Download specific version of commons-cli that Flink 1.9.3 uses
RUN wget https://repo1.maven.org/maven2/commons-cli/commons-cli/1.4/commons-cli-1.4.jar -P /opt/flink/lib/

# Copy only the necessary Hadoop JARs (avoiding commons-cli from Hadoop)
RUN mkdir -p /opt/flink/lib/hadoop && \
    cp $HADOOP_HOME/share/hadoop/common/hadoop-common-3.2.1.jar /opt/flink/lib/hadoop/ && \
    cp $HADOOP_HOME/share/hadoop/hdfs/hadoop-hdfs-3.2.1.jar /opt/flink/lib/hadoop/

# Set the classpath to ensure Flink uses the correct commons-cli
ENV FLINK_ENV_JAVA_OPTS="-Djava.class.path=/opt/flink/lib/commons-cli-1.4.jar:/opt/flink/lib/*:/opt/flink/lib/hadoop/*"