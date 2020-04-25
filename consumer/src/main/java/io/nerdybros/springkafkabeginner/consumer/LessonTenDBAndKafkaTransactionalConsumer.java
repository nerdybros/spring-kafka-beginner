package io.nerdybros.springkafkabeginner.consumer;

import io.nerdybros.springkafkabeginner.consumer.domain.MyEvent;
import io.nerdybros.springkafkabeginner.consumer.store.EventStore;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.transaction.ChainedKafkaTransactionManager;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.util.backoff.FixedBackOff;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

/**
 * 10강: JPA DB 핸들링과 Kafka 처리를 하나의 Transaction으로 처리
 */
@Component
public class LessonTenDBAndKafkaTransactionalConsumer {
    private final String groupId = "test-group-tx-3";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaTransactionManager<String, String> kafkaTransactionManager;

    @Autowired
    private EntityManagerFactory entityManagerFactory;

    @Autowired
    private EventStore store;

    @KafkaListener(topics = { "test-topic-tx-db" }, containerFactory = "dbAndKafkaTransactionalListenerContainerFactory", groupId = groupId)
    public void listen(Message<String> message) {
        System.out.println("[" + groupId + "] simple jpa and kafka transactional consumer : " + message);
        System.out.println(kafkaTemplate.isTransactional());
        kafkaTemplate.send("test-topic-tx-new", message.getPayload().toUpperCase());

        MessageHeaders headers = message.getHeaders();
        MyEvent event = new MyEvent();
        String id = headers.get(KafkaHeaders.RECEIVED_TOPIC) + "-" + headers.get(KafkaHeaders.RECEIVED_PARTITION_ID) + "-" + headers.get(KafkaHeaders.OFFSET);
        event.setId(id);
        event.setContent(message.getPayload());
        this.store.save(event);

        Integer offset = Integer.valueOf(headers.get(KafkaHeaders.OFFSET).toString());
        if (offset % 2 == 0) {
            throw new RuntimeException("bad consumer");
        }
    }

    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean("dbAndKafkaTransactionalListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> simpleKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(this.consumerFactory());
        factory.getContainerProperties().setTransactionManager(this.chainedKafkaTransactionManager(kafkaTransactionManager, this.jpaTransactionManager(this.entityManagerFactory)));
        factory.setAfterRollbackProcessor(new DefaultAfterRollbackProcessor<>(new FixedBackOff(0l, 0l)));
        return factory;
    }

    ChainedKafkaTransactionManager<String, String> chainedKafkaTransactionManager(KafkaTransactionManager<String, String> kafkaTransactionManager, JpaTransactionManager dataSourceTransactionManager) {
        kafkaTransactionManager.setTransactionSynchronization(AbstractPlatformTransactionManager.SYNCHRONIZATION_ON_ACTUAL_TRANSACTION);
        return new ChainedKafkaTransactionManager<>(kafkaTransactionManager, dataSourceTransactionManager);
    }

    public JpaTransactionManager jpaTransactionManager(EntityManagerFactory entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory);
    }
}
