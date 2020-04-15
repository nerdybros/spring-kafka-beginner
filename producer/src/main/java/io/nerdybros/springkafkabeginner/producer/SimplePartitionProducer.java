package io.nerdybros.springkafkabeginner.producer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.HashMap;
import java.util.Map;

@Component
public class SimplePartitionProducer {

    // Create topic with multiple partitions
    // ./kafka-topics.sh --zookeeper localhost:2181 --create --topic test-topic-mp --partitions 3 --replication-factor 1

    @Value("${kafka.topic-mp:sample-topic-mp}")
    private String topic;

    @Autowired
    @Qualifier("simplePartitionProducerKafkaTemplate")
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String key, String message) {

        /* Message with same keys will be guaranteed to be in same partition by Kafka*/
        // using key
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, message);

        // using Message<?> - set in Headers
        Message<String> record = MessageBuilder.withPayload(message)
                                    .setHeader(KafkaHeaders.TOPIC, topic)
                                    .setHeader(KafkaHeaders.MESSAGE_KEY, key)
                                    .build();
        ListenableFuture<SendResult<String, String>> futureUsingMessage = kafkaTemplate.send(record);


        /* Assign partitions explicitly */
        // users can assign partition id (note: partition starts at 0)
        ListenableFuture<SendResult<String, String>> futurePartitionOne = kafkaTemplate.send(topic, 1, key, message);

        // similarly, it is possible to set partition id in Message<?> headers
        Message<String> recordWithPartition = MessageBuilder.fromMessage(record)
                                                .setHeader(KafkaHeaders.PARTITION_ID, 1)
                                                .build();
        ListenableFuture<SendResult<String, String>> futureUsingMessageAndPartition = kafkaTemplate.send(recordWithPartition);
    }

    public void sendMessageNoMessageKey(String message) {
        Message<String> record = MessageBuilder.withPayload(message)
                                            .setHeader(KafkaHeaders.TOPIC, topic)
                                            .build();
        ListenableFuture<SendResult<String, String>> send = kafkaTemplate.send(record);
    }

    @Bean(name = "simplePartitionProducerKafkaTemplate")
    public KafkaTemplate<String, String> kafkaTemplate() {
        // producer configuration
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // producer factory
        DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(props);

        // kafka template
        return new KafkaTemplate<>(producerFactory);
    }

    // test
    // ./kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic test-topic-mp
}