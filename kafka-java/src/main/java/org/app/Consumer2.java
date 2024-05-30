package org.app;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class Consumer2 {
    static final Logger LOGGER = LoggerFactory.getLogger(Consumer2.class);
    static final String bootstrapServer = "localhost:9092,localhost:9093";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "Consumer-group-2");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty("security.protocol", "SASL_PLAINTEXT");
        properties.setProperty("sasl.mechanism", "PLAIN");
//        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"client\" password=\"client123\";");
        System.setProperty("java.security.auth.login.config", "/home/surjith-pt7589/Current/kafka_2.12-3.7.0/kafka_2.12-3.7.0/config/kafka_client_jaas.conf");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(List.of("sasl-plaintext"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(2);
            for (ConsumerRecord record : records) {
                LOGGER.info("\nRecieved record: \n" +
                        "Key: " + record.key() + ", " +
                        "Value: " + record.value() + ", " +
                        "Topic: " + record.topic() + ", " +
                        "Partition: " + record.partition() + ", " +
                        "Offset: " + record.offset() + "\n");
            }
        }
    }
}
/*
*
When a single node in a KRaft cluster needs to restart after all other nodes have shut down, it needs to ensure it can safely rejoin the cluster without causing any inconsistencies or disruptions. Here are some key factors the restarting node needs to consider:
    1. Time of Joining: The restarting node needs to know the time when it initially joined the cluster. This information helps determine its position in the Raft log and ensures that it does not fall behind or diverge from the current state of the cluster.
    2. Last Known Activity: The restarting node should have knowledge of its last known activity or interaction with the other nodes in the cluster. This includes the last communication with the current leader and any pending transactions or operations that were in progress before shutdown.
    3. Cluster Configuration: The restarting node needs to be aware of the current configuration of the cluster, including the number and identities of other nodes, their roles (leader, follower, candidate), and any recent changes to the cluster membership.
    4. Commitment and Consistency: Before rejoining the cluster, the restarting node must ensure that it has committed all pending changes to the Raft log and maintained consistency with the majority of the nodes in the cluster. This helps prevent data loss or inconsistencies upon rejoining.
    5. Leader Election Process: If the restarting node was previously the leader or a candidate for leader election, it should participate in the leader election process again to determine the new leader based on the current state of the cluster.
    6. State Synchronization: In case the restarting node has fallen behind in the Raft log or missed updates while offline, it should synchronize its state with the rest of the cluster by requesting and applying missing log entries from other nodes.
    7. Health Checks and Recovery: The restarting node should perform health checks to ensure its own integrity and readiness before attempting to rejoin the cluster. It should recover any corrupted or incomplete data structures and ensure that it can handle incoming requests properly.
By considering these factors, the restarting node can safely rejoin the KRaft cluster and resume its role in maintaining consensus and coordination among the nodes.

*
*
*
* */