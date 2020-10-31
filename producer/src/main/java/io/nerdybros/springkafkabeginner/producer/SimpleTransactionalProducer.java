package io.nerdybros.springkafkabeginner.producer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;

@Component
@EnableTransactionManagement
public class SimpleTransactionalProducer {

    @Value("${kafka.topic-tx:sample-topic}")
    private String topic;

    @Autowired
    @Qualifier("transactionalProducerFactory")
    private DefaultKafkaProducerFactory<String, String> producerFactory;

    @Autowired
    @Qualifier("simpleTransactionalProducerKafkaTemplate")
    private KafkaTemplate<String, String> kafkaTemplate;

    @Transactional
    public void sendMessageUsingAnnotation(List<String> messages) {
        System.out.println(kafkaTemplate.isTransactional());
        for (String message: messages) {
            kafkaTemplate.send(topic, message);
        }
    }

    public void sendMessageUsingMethod(List<String> messages) {
        System.out.println(kafkaTemplate.isTransactional());
        kafkaTemplate.executeInTransaction(callback -> {
            for (String message: messages) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
                callback.send(record);
            }
            return null;
        });
    }

    @Bean
    public KafkaTransactionManager<String, String> kafkaTransactionManager() {
        return new KafkaTransactionManager<>(producerFactory);
    }

    @Bean(name = "simpleTransactionalProducerKafkaTemplate")
    public KafkaTemplate<String, String> kafkaTemplate() {
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory);
        return kafkaTemplate;
    }

    @Bean(name = "transactionalProducerFactory")
    public DefaultKafkaProducerFactory<String, String> globalProducerFactory() {
        // producer configuration
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx-id-");

        // producer factory
        DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(props);
        return producerFactory;
    }
}
