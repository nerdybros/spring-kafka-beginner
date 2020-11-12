package io.nerdybros.springkafkabeginner.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

@Component
@DependsOn("simpleProducer")
public class LessonFourSimplePartitionProducer {

    // Create topic with multiple partitions
    // ./kafka-topics.sh --zookeeper localhost:2181 --create --topic test-topic-mp --partitions 3 --replication-factor 1

    @Value("${kafka.topic-mp:sample-topic-mp}")
    private String topic;

    @Autowired
    @Qualifier("simpleProducerKafkaTemplate")
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String key, String message) {
        /* Message with same keys will be guaranteed to be in same partition by Kafka */
        // using key
        kafkaTemplate.send(topic, key, message);

        // using MESSAGE_KEY header in Message<?>
        Message<String> record = MessageBuilder.withPayload(message)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(KafkaHeaders.MESSAGE_KEY, key)
                .build();
        kafkaTemplate.send(record);


        /* 참고 - Assign partitions explicitly */
        // users can assign partition id (note: partition starts at 0)
        kafkaTemplate.send(topic, 1, key, message);

        // similarly, it is possible to set partition id in Message<?> headers
        Message<String> recordWithPartition = MessageBuilder.fromMessage(record)
                .setHeader(KafkaHeaders.PARTITION_ID, 1)
                .build();
        kafkaTemplate.send(recordWithPartition);
    }

    public void sendMessageNoMessageKey(String message) {
        Message<String> record = MessageBuilder.withPayload(message)
                                            .setHeader(KafkaHeaders.TOPIC, topic)
                                            .build();
        kafkaTemplate.send(record);
    }

//    @Bean(name = "simplePartitionProducerKafkaTemplate")
//    public KafkaTemplate<String, String> kafkaTemplate() {
//        // producer configuration
//        Map<String, Object> props = new HashMap<>();
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
//        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//
//        // producer factory
//        DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(props);
//
//        // kafka template
//        return new KafkaTemplate<>(producerFactory);
//    }

    // test
    // ./kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic test-topic-mp
}
