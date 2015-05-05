package kafka.consumer;

import com.google.common.collect.ImmutableMap;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

//import kafka.etl.FetchRequest;

public class KafkaContext implements Closeable {

    private static Logger LOG = LoggerFactory.getLogger(KafkaContext.class);

    SimpleConsumer consumer;
    String topic;
    int partition;
    long startOffset = -1L;
    long lastOffset = -1L;
    long curOffset;
    int fetchSize;
    ByteBufferMessageSet messages;
    Iterator<MessageAndOffset> iterator;
    final ArrayBlockingQueue<ByteBufferMessageSet> queue;
    final FetchThread fetcher;

    public KafkaContext(String broker, String topic,
                        int partition, long lastCommit, int fetchSize, int timeout, int bufferSize,
                        String reset) {
        LOG.info("Broker: " + broker);
        String[] sp = broker.split(":"); // broker-id:host:port
        consumer = new SimpleConsumer(sp[1], Integer.valueOf(sp[2]), timeout, bufferSize, "KafkaContext");
        this.topic = topic;
        this.partition = partition;
        this.startOffset = lastCommit;
        this.curOffset = getStartOffset();
        this.lastOffset = getLastOffset();
        this.fetchSize = fetchSize;


        resetOffset(reset, sp[0], partition);


        queue = new ArrayBlockingQueue<ByteBufferMessageSet>(5);
        fetcher = new FetchThread(consumer, queue, topic, partition, curOffset, fetchSize);
        fetcher.start();
    }

    private void resetOffset(String reset, String brokerId, int partition) {
        if (reset == null) return;
        LOG.info("RESET {} {} {}", new Object[]{reset, brokerId, partition});
        if (reset.indexOf(":") > 0) {
            String[] sp = reset.split(":");
            if (!sp[0].equals(brokerId + "-" + partition)) {
                return;
            }
            reset = sp[1];
        }
        if ("smallest".equals(reset)) {
            setStartOffset(-1);
        } else if ("largest".equals(reset)) {
            setStartOffset(lastOffset);
        } else {
            try {
                setStartOffset(Long.valueOf(reset));
            } catch (NumberFormatException e) {
            }
        }
    }

    //@Override
    public void close() throws IOException {
        fetcher.stop = true;
        //fetcher.interrupt();
        while (!fetcher.stopped) ;
        consumer.close();
    }

    private boolean hasMore() {
        if (iterator == null) {
            fetchMore();
            if (iterator == null) {
                return false;
            }
        }
        boolean hasNext = iterator.hasNext();
        if (hasNext) return hasNext;
        else if (curOffset >= lastOffset) return false;
        else {
            fetchMore();
            return iterator.hasNext();
        }
    }

    private void fetchMore() {

        while (!fetcher.stop || !queue.isEmpty()) {
            messages = queue.poll();
            if (messages != null) {
                //int code = messages.getErrorCode();
                //if (code != 0) {
                //    ErrorMapping.maybeThrowException(code);
                //}
                iterator = messages.iterator();
                break;
            }
        }
    }

    public long getNext(LongWritable key, BytesWritable value) throws IOException {
        if (!hasMore()) return -1L;

        MessageAndOffset messageOffset = iterator.next();
        Message message = messageOffset.message();

        key.set(curOffset);
        curOffset = messageOffset.offset();

        //byte[] bytes = new byte[message.payloadSize()];
        //message.payload().get(bytes);
        //value.set(bytes, 0, message.payloadSize());
        ByteBuffer buffer = message.payload();
        value.set(buffer.array(), buffer.arrayOffset(), message.payloadSize());

        return curOffset;
    }

    public long getStartOffset() {
        if (startOffset <= 0) {
            startOffset = consumer.getOffsetsBefore(toOffsetRequest(topic, partition, -2L, 1)).offsets(topic, partition)[0];
        }
        return startOffset;
    }

    public void setStartOffset(long offset) {
        if (offset <= 0) {
            offset = consumer.getOffsetsBefore(toOffsetRequest(topic, partition, -2L, 1)).offsets(topic, partition)[0];
            LOG.info("Smallest Offset {}", offset);
        }
        curOffset = startOffset = offset;
    }

    public long getLastOffset() {
        if (lastOffset <= 0) {
            lastOffset = consumer.getOffsetsBefore(toOffsetRequest(topic, partition, -1L, 1)).offsets(topic, partition)[0];
        }
        return lastOffset;
    }

    static OffsetRequest toOffsetRequest(final String topic, final int partitionNum, final long asOfTime,
                                         final int numOffsets) {
        final TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partitionNum);
        final PartitionOffsetRequestInfo partitionInfoReq = new PartitionOffsetRequestInfo(asOfTime, numOffsets);
        final Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = ImmutableMap.of(topicAndPartition, partitionInfoReq);
        return new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), "KafkaContext");
    }

    static class FetchThread extends Thread {

        String topic;
        int partition;
        long offset;
        int fetchSize;
        SimpleConsumer consumer;
        public volatile boolean stop = false;
        public volatile boolean stopped = false;
        ArrayBlockingQueue<ByteBufferMessageSet> queue;
        boolean hasData = false;
        ByteBufferMessageSet messages = null;

        public FetchThread(SimpleConsumer consumer, ArrayBlockingQueue<ByteBufferMessageSet> queue,
                           String topic, int partition, long offset, int fetchSize) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.fetchSize = fetchSize;
            this.consumer = consumer;
            this.queue = queue;
        }

        @Override
        public void run() {
            while (!stop) {
                if (messages == null) {
                    FetchRequest request =
                            new FetchRequestBuilder()
                                    .addFetch(topic, partition, offset, fetchSize).clientId("KafkaContext").build();

                    LOG.info("fetching offset {}", offset);
                    final kafka.javaapi.FetchResponse fetchResponse = consumer.fetch(request);
                    if (fetchResponse.hasError()) {
                        final short code = fetchResponse.errorCode(topic, partition);
                        if (hasData && code == ErrorMapping.OffsetOutOfRangeCode()) {
                            // no more data
                            //queue.notify();
                            stop = true;
                            LOG.info("No More Data");
                        } else {
                            while (!queue.offer(messages)) ;
                            stop = true;
                        }
                    } else {
                        messages = fetchResponse.messageSet(topic, partition);
                        if (!queue.offer(messages)) {
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                            }
                            continue;
                        }
                        hasData = true;
                        offset += messages.validBytes(); // next offset to fetch
                        //LOG.info("Valid bytes {} {}", messages.validBytes(), stop);
                        messages = null;
                    }

                }

            }
            stopped = true;
        }
    }

}
