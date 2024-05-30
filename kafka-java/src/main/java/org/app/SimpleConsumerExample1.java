package org.app;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
//import java.util.concurrent.ThreadLocalRandom;

public class SimpleConsumerExample1 {
    static final Logger LOGGER = LoggerFactory.getLogger(SimpleConsumerExample1.class);
    static final String topic = "test-plain";
    static final List<String> brokers = List.of("localhost:9092", "localhost:9093", "localhost:9094");
    static final int fetchSize = 1024 * 1024;
    static final int soTimeout = 10000;
    static final String clientId = "";
    static final int maxRetries = 0;
    static final int numPartitions = 2;
    static ExecutorService executorService = Executors.newFixedThreadPool(2);


    public static void main(String[] args) {
        executeFunction();
    }

    private static void executeFunction() {
        for (int partition = 0; partition < numPartitions; partition++) {
            int partitionId = partition;
            executorService.execute(() -> fetchMessagesFromBrokers(0, partitionId));
        }
//        executorService.shutdownNow();

    }

    private static void fetchMessagesFromBrokers(int brokerIndex, int partition) {
        if (brokerIndex >= brokers.size()) {
            executorService.shutdownNow();
            System.out.println("====================================================================================================================================\n\n");
            executeFunction();
            return;
        }
        String broker = brokers.get(brokerIndex);
        System.out.println("========================" + broker + "<--->" + partition + "====================\n\n");
        fetchMessages(broker, partition);
        fetchMessagesFromBrokers(brokerIndex + 1, partition);
    }

    private static void fetchMessages(String broker, int partition) {
        String[] hostPort = broker.split(":");
        String brokerHost = hostPort[0];
        int brokerPort = Integer.parseInt(hostPort[1]);

        long offset = 0;
        int numErrors = 0;

        while (true) {
            SimpleConsumer consumer = new SimpleConsumer(brokerHost, brokerPort, soTimeout, fetchSize, clientId);

            FetchRequest request = new FetchRequestBuilder().clientId(clientId).addFetch(topic, partition, offset, fetchSize).build();
            FetchResponse response = null;

            try {
                response = consumer.fetch(request);
            } catch (Exception e) {
                LOGGER.error("Error fetching data from broker {}:{} Reason: ", brokerHost, brokerPort, e);
                consumer.close();
                break;
//                if (numErrors++ > maxRetries) {
//                    consumer.close();
//                    return;
//                }
//                continue;
            }

            if (response.hasError()) {
                short code = response.errorCode(topic, partition);
                LOGGER.error("Error fetching data from broker {}:{} Reason: {}", brokerHost, brokerPort, code);
                consumer.close();
                break;

//                if (numErrors++ > maxRetries) {
//                    consumer.close();
//                    return;
//                }
//                if (code == kafka.common.ErrorMapping.OffsetOutOfRangeCode()) {
//                    offset = 0;
//                }
//                continue;
            }

            numErrors = 0;
            ByteBufferMessageSet messageSet = response.messageSet(topic, partition);
            for (MessageAndOffset messageAndOffset : messageSet) {
                long currentOffset = messageAndOffset.offset();
                System.out.println("================" + currentOffset + "==================");
                if (currentOffset < offset) {
                    LOGGER.info("Found an old offset: " + currentOffset + " Expecting: " + offset);
                    continue;
                }
//                if(currentOffset==0) return;

                offset = messageAndOffset.nextOffset();
                ByteBuffer payload = messageAndOffset.message().payload();

                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                String message = new String(bytes);
                System.out.println(partition);
                LOGGER.info("Received message: " + message + " Offset: " + messageAndOffset.offset());
                System.out.println("Consumed Offset: " + messageAndOffset.offset() + ", Message: " + message);
//                try {
//                    Thread.sleep(1000);
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
            }
//            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//                LOGGER.info("ShutDown the Current Thread...!!!!");
//                executorService.shutdownNow();
//            }));
            consumer.close();
//            break;
//            break;
        }
    }
}
