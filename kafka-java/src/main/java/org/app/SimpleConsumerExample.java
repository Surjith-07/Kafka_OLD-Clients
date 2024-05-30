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
import java.util.concurrent.TimeUnit;

public class SimpleConsumerExample {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleConsumerExample.class);
    private static final String TOPIC = "test-plain";
    private static final List<String> BROKERS = List.of("localhost:9092", "localhost:9093", "localhost:9094");
    private static final int FETCH_SIZE = 1024 * 1024;
    private static final int SO_TIMEOUT = 10000;
    private static final String CLIENT_ID = "SimpleConsumerExample";
    private static final int MAX_RETRIES = 3; // Maximum number of retries for fetching messages
    private static ExecutorService executorService = Executors.newFixedThreadPool(2);

    public static void main(String[] args) {
        executeFunction();
    }

    private static void executeFunction() {
        for (int partition = 0; partition < getNumPartitions(); partition++) {
            int partitionId = partition;
            executorService.execute(() -> fetchMessagesFromBrokers(0, partitionId));
        }
        // Add shutdown hook to gracefully shut down the consumer
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutting down the consumer...");
            executorService.shutdown();
            try {
                executorService.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOGGER.error("Error while shutting down the consumer: {}", e.getMessage());
            }
        }));
    }

    private static int getNumPartitions() {
        // Implement logic to dynamically determine the number of partitions for the topic
        return 2; // Example: Return the fixed number of partitions for now
    }

    private static void fetchMessagesFromBrokers(int brokerIndex, int partition) {
        if (brokerIndex >= BROKERS.size()) {
            LOGGER.warn("All brokers failed to fetch messages from partition {}", partition);
            executeFunction();
            return;
        }
        String broker = BROKERS.get(brokerIndex);
        LOGGER.info("Fetching messages from broker {} for partition {}", broker, partition);
        fetchMessages(broker, partition);
        fetchMessagesFromBrokers(brokerIndex + 1, partition);
    }

    private static void fetchMessages(String broker, int partition) {
        String[] hostPort = broker.split(":");
        String brokerHost = hostPort[0];
        int brokerPort = Integer.parseInt(hostPort[1]);

        long offset = 0;
        int numErrors = 0;

        while (numErrors <= MAX_RETRIES) {
            SimpleConsumer consumer = new SimpleConsumer(brokerHost, brokerPort, SO_TIMEOUT, FETCH_SIZE, CLIENT_ID);

            FetchRequest request = new FetchRequestBuilder()
                    .clientId(CLIENT_ID)
                    .addFetch(TOPIC, partition, offset, FETCH_SIZE)
                    .build();
            FetchResponse response;
            try {
                response = consumer.fetch(request);
            } catch (Exception e) {
                LOGGER.error("Error fetching data from broker {}:{}: {}", brokerHost, brokerPort, e.getMessage());
                consumer.close();
                numErrors++;
                continue;
            }

            if (response.hasError()) {
                short code = response.errorCode(TOPIC, partition);
                LOGGER.error("Error fetching data from broker {}:{} - Error code: {}", brokerHost, brokerPort, code);
                consumer.close();
                numErrors++;
                continue;
            }

            numErrors = 0;
            ByteBufferMessageSet messageSet = response.messageSet(TOPIC, partition);
            for (MessageAndOffset messageAndOffset : messageSet) {
                long currentOffset = messageAndOffset.offset();
                if (currentOffset < offset) {
                    LOGGER.info("Found an old offset: {} Expecting: {}", currentOffset, offset);
                    continue;
                }
                offset = messageAndOffset.nextOffset();
                ByteBuffer payload = messageAndOffset.message().payload();
                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                String message = new String(bytes);
                LOGGER.info("Received message: {} Offset: {}", message, messageAndOffset.offset());
            }
            consumer.close();
        }
        LOGGER.warn("Max retries reached for fetching messages from broker {} for partition {}", broker, partition);
    }
}
