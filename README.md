kafka-hadoop-consumer
=====================

A Hadoop workload for consuming data from Kafka. 100% Map, 0% Reduce

Usage:

0. Build the java .jar using maven: mvn clean install
1. Copy that kafka-hadoop-consumer-0.1.0.jar to the working\share directory
2. Copy the working directory to ~/working on your host machine
3. Run sh ~/working/docker-up.sh
4. Run sh share/consume.sh from inside the Hadoop container
5. ᕕ( ᐛ )ᕗ
