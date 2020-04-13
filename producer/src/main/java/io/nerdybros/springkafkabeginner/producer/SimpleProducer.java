package io.nerdybros.springkafkabeginner.producer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.HashMap;
import java.util.Map;

@Component
public class SimpleProducer {

    @Value("${kafka.topic}")
    private String topic;

    @Autowired
    @Qualifier("simpleProducerKafkaTemplate")
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String message) {
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, message);
    }

    @Bean(name = "simpleProducerKafkaTemplate")
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
}
