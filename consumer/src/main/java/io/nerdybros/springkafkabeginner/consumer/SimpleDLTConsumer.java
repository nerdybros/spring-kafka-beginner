package io.nerdybros.springkafkabeginner.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Component
public class SimpleDLTConsumer {

    private final String groupId = "test-group-dlt";

    @KafkaListener(topics = { "test-topic" }, containerFactory = "simpleDLTListenerContainerFactory", groupId = groupId)
    public void listen(String message) {
        System.out.println("[" + groupId + "] simple dlt consumer : " + message);
        // handle business
        throw new RuntimeException("something bad happened");
    }

    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean("simpleDLTListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> simpleKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(this.consumerFactory());

        /* Retries using SeekToCurrentErrorHandler */
        FixedBackOff fixedBackOff = new FixedBackOff();
        fixedBackOff.setInterval(1000l);
        fixedBackOff.setMaxAttempts(3);

        /* Dead Letter Topic */
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(props);
        KafkaTemplate<String, String> template = new KafkaTemplate<>(producerFactory);

        // defaults to topic.DLT topic
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
        // SeekToCurrentErrorHandler errorHandler = new SeekToCurrentErrorHandler(recoverer, fixedBackOff);

        // more advanced
        DeadLetterPublishingRecoverer moreAdvancedRecoverer = new DeadLetterPublishingRecoverer(template, (record, exception) -> {
            if (record.offset() % 2 == 0) {
                return new TopicPartition(record.topic() + "-even-dlt", record.partition());
            }
            else {
                return new TopicPartition(record.topic() +  "-odd-dlt", record.partition());
            }
        });

        SeekToCurrentErrorHandler errorHandler = new SeekToCurrentErrorHandler(moreAdvancedRecoverer, fixedBackOff);
        factory.setErrorHandler(errorHandler);
        factory.setStatefulRetry(true);

        return factory;
    }
}
