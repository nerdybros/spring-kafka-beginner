package io.nerdybros.springkafkabeginner.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * 4강: 에러 핸들링 & RetryTemplate을 사용한 Consumer retry
 */
@Component
public class LessonFourRetryingConsumer {

    private final String groupId = "test-group-retry";

    @KafkaListener(topics = { "test-topic-retry" }, containerFactory = "retryingListenerContainerFactory", groupId = groupId)
    public void listen(String message) {
        System.out.println("[" + groupId + "] simple retrying consumer : " + message);
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

    @Bean("retryingListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> simpleKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(this.consumerFactory());

        /* Retries using RetryTemplate */
        RetryTemplate retryTemplate = new RetryTemplate();
        // set how many milliseconds next try should be started
        FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy(); // or ExponentialBackOffPolicy can be used
        backOffPolicy.setBackOffPeriod(1000l);
        retryTemplate.setBackOffPolicy(backOffPolicy);

        // set maximum attempts
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
        retryPolicy.setMaxAttempts(3);
        retryTemplate.setRetryPolicy(retryPolicy);

        factory.setRetryTemplate(retryTemplate);

        return factory;
    }
}
