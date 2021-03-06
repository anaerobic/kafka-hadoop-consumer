package kafka.consumer;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ZkUtils implements Closeable {

    private static Logger LOG = LoggerFactory.getLogger(ZkUtils.class);

    private static final String CONSUMERS_PATH = "/consumers";
    private static final String BROKER_IDS_PATH = "/brokers/ids";
    private static final String BROKER_TOPICS_PATH = "/brokers/topics";
    
    /*
     * class ZKGroupDirs(val group: String) {
  def consumerDir = ZkUtils.ConsumersPath
  def consumerGroupDir = consumerDir + "/" + group
  def consumerRegistryDir = consumerGroupDir + "/ids"
}

class ZKGroupTopicDirs(group: String, topic: String) extends ZKGroupDirs(group) {
  def consumerOffsetDir = consumerGroupDir + "/offsets/" + topic
  def consumerOwnerDir = consumerGroupDir + "/owners/" + topic
}
     */

    private ZkClient client;
    Map<String, String> brokers;

    public ZkUtils(Configuration config) {
        connect(config);
    }

    private void connect(Configuration config) {
        String zk = config.get("kafka.zk.connect");
        int stimeout = config.getInt("kafka.zk.session.timeout.ms", 10000);
        int ctimeout = config.getInt("kafka.zk.connection.timeout.ms", 10000);
        client = new ZkClient(zk, stimeout, ctimeout, new StringSerializer());
    }

    public String getBroker(String id) {
        if (brokers == null) {
            brokers = new HashMap<String, String>();
            List<String> brokerIds = getChildrenParentMayNotExist(BROKER_IDS_PATH);
            for (String bid : brokerIds) {
                // data = {"jmx_port":-1,"timestamp":"1430776217789","host":"kafka.lacolhost.com","version":1,"port":9092}
                String data = client.readData(BROKER_IDS_PATH + "/" + bid);
                LOG.info("Broker " + bid + " " + data);

                final String[] splitOnCommas = data.split(",");

                final String[] hostSplit = splitOnCommas[2].split(":");
                final String[] portSplit = splitOnCommas[4].split(":");

                final String host = hostSplit[1].replace("\"", "");
                final Integer port = Integer.parseInt(portSplit[1].replace("}", ""));
                
                brokers.put(bid, host + ":" + port);
            }
        }
        return brokers.get(id);
    }

    public List<String> getPartitions(String topic) {
        List<String> partitions = new ArrayList<String>();
        List<String> brokersTopics = getChildrenParentMayNotExist(BROKER_TOPICS_PATH + "/" + topic);
        for (String broker : brokersTopics) {
            final String topicBrokerIdPath = BROKER_TOPICS_PATH + "/" + topic + "/partitions/" + 0; //broker
            //String parts = client.readData(topicBrokerIdPath);
            LOG.info("Path: " + topicBrokerIdPath);

            partitions.add("0-0");
        //  for (int i = 0; i < Integer.valueOf(parts); i++) {
        //      partitions.add(broker + "-" + i);
        //  }
        }
        return partitions;
    }

    private String getOffsetsPath(String group, String topic, String partition) {
        return CONSUMERS_PATH + "/" + group + "/offsets/" + topic + "/" + partition;
    }

    private String getTempOffsetsPath(String group, String topic, String partition) {
        return CONSUMERS_PATH + "/" + group + "/offsets-temp/" + topic + "/" + partition;
    }

    private String getTempOffsetsPath(String group, String topic) {
        return CONSUMERS_PATH + "/" + group + "/offsets-temp/" + topic;
    }


    public long getLastCommit(String group, String topic, String partition) {
        String znode = getOffsetsPath(group, topic, partition);
        String offset = client.readData(znode, true);

        if (offset == null) {
            return -1L;
        }
        return Long.valueOf(offset);
    }

    public void setLastCommit(String group, String topic, String partition, long commit, boolean temp) {
        String path = temp ? getTempOffsetsPath(group, topic, partition)
                : getOffsetsPath(group, topic, partition);
        if (!client.exists(path)) {
            client.createPersistent(path, true);
        }
        client.writeData(path, commit);
    }

    public boolean commit(String group, String topic) {
        List<String> partitions = getChildrenParentMayNotExist(getTempOffsetsPath(group, topic));
        for (String partition : partitions) {
            String path = getTempOffsetsPath(group, topic, partition);
            String offset = client.readData(path);
            setLastCommit(group, topic, partition, Long.valueOf(offset), false);
            client.delete(path);
        }
        return true;
    }


    private List<String> getChildrenParentMayNotExist(String path) {
        try {
            List<String> children = client.getChildren(path);
            return children;
        } catch (ZkNoNodeException e) {
            return new ArrayList<String>();
        }
    }

    //@Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    static class StringSerializer implements ZkSerializer {

        public StringSerializer() {
        }

        //@Override
        public Object deserialize(byte[] data) throws ZkMarshallingError {
            if (data == null) return null;
            return new String(data);
        }

        //@Override
        public byte[] serialize(Object data) throws ZkMarshallingError {
            return data.toString().getBytes();
        }

    }

}
