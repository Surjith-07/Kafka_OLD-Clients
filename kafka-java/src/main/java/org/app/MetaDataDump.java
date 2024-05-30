package org.app;

import kafka.cluster.BrokerEndPoint;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class MetaDataDump {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetaDataDump.class);

    public static void main(String[] args) {
        List<String> brokers = Arrays.asList("localhost:9093", "localhost:9093", "localhost:9094");
        List<String> topics = Arrays.asList("test-plain");
        boolean fetched = false;

        for (String broker : brokers) {
            String[] hostPort = broker.split(":");
            String brokerHost = hostPort[0];
            int brokerPort = Integer.parseInt(hostPort[1]);

            SimpleConsumer consumer = new SimpleConsumer(brokerHost, brokerPort, 100000, 64 * 1024, "metadata-fetcher");
            TopicMetadataRequest req = new TopicMetadataRequest(topics);

            try {
                TopicMetadataResponse resp = consumer.send(req);
                List<kafka.javaapi.TopicMetadata> data = resp.topicsMetadata();
                for (kafka.javaapi.TopicMetadata item : data) {
                    System.out.println("Topic: " + item.topic());
                    for (kafka.javaapi.PartitionMetadata part : item.partitionsMetadata()) {
                        StringBuilder replicas = new StringBuilder();
                        StringBuilder isr = new StringBuilder();
                        for (BrokerEndPoint replica : part.replicas()) {
                            replicas.append(" ").append(replica.port());
                        }
                        for (BrokerEndPoint replica : part.isr()) {
                            isr.append(" ").append(replica.port());
                        }
                        System.out.println("    Partition: " + part.partitionId() + ": Leader: " + part.leader().port() +
                                " Replicas:[" + replicas + "] ISR:[" + isr + "]\n");
                    }
                }
                fetched = true;
            } catch (Exception e) {
                LOGGER.error("Error fetching metadata from broker {}:{}", brokerHost, brokerPort, e);
            } finally {
                consumer.close();
            }

            if (fetched) {
                break;
            }
        }

        if (!fetched) {
            System.out.println("Failed to fetch metadata from all brokers.");
        }
    }
}
