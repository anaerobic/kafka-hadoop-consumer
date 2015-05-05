cd $HADOOP_PREFIX

bin/hdfs dfsadmin -safemode leave

bin/hadoop dfs -mkdir /hdfs

bin/hadoop jar /share/kafka-hadoop-consumer-0.1.0.jar kafka.consumer.HadoopConsumer -z zookeeper.lacolhost.com:2181 -t results /hdfs

bin/hdfs dfs -cat /hdfs/*