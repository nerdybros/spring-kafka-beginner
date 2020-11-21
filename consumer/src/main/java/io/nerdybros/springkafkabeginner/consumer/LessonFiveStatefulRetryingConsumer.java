package io.nerdybros.springkafkabeginner.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * 5강: Consumer stateful retry
 */
@Component
public class LessonFiveStatefulRetryingConsumer {

    private final String groupId = "test-group-stateful-retry";

    @KafkaListener(topics = { "test-topic-stateful-retry" }, containerFactory = "simpleStatefulRetryingListenerContainerFactory", groupId = groupId)
    public void listen(String message) {
        System.out.println("[" + groupId + "] simple stateful retrying consumer : " + message);
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

    @Bean("simpleStatefulRetryingListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> simpleKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(this.consumerFactory());

        /* Retries using SeekToCurrentErrorHandler */
        FixedBackOff fixedBackOff = new FixedBackOff();
        fixedBackOff.setInterval(1000l);
        fixedBackOff.setMaxAttempts(3);

        SeekToCurrentErrorHandler errorHandler = new SeekToCurrentErrorHandler((consumerRecord, e) -> {
            System.out.println("failed record: " + consumerRecord.toString() + " , reason: " + e.getMessage());
            // customize what error handler should do here
        }, fixedBackOff);
        factory.setErrorHandler(errorHandler);
        factory.setStatefulRetry(true);

        return factory;
    }
}
