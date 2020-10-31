package io.nerdybros.springkafkabeginner.consumer;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.stereotype.Component;

/**
 * 11강: Consumer Consumer rebalancing
 */
@Component
public class LessonElevenRebalanceConsumer {

	// partition add
	// kafka-topics.bat --alter --zookeeper localhost:2181 --topic test-topic-rebalance --partitions 3

	// producer cli
	// kafka-console-producer.bat --broker-list localhost:9092 --topic test-topic-rebalance

	private final String groupId = "test-group-rebalance";

	@KafkaListener(topics = { "test-topic-rebalance" }, containerFactory = "simpleRebalancingListenerContainerFactory", groupId = groupId)
	public void listenWithDelay(String message) {
		System.out.println("### listenWithDelay, message: " + message);
		System.out.println("### start time sleep ###");
		// time delay itentionally to rebalacne partition
		for (int i = Integer.MIN_VALUE; i < Integer.MAX_VALUE; i++) {
			for (int j = 0; j < 20; j++)
				;
		}
		System.out.println("### end time sleep ###");
	}

	@KafkaListener(topics = { "test-topic-rebalance" }, containerFactory = "simpleRebalancingListenerContainerFactory", groupId = groupId)
	public void listen(String message) {
		System.out.println("### listen, message: " + message);
	}

	public ConsumerFactory<String, String> consumerFactory() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 5000);
		return new DefaultKafkaConsumerFactory<>(props);
	}

	@Bean("simpleRebalancingListenerContainerFactory")
	public ConcurrentKafkaListenerContainerFactory<String, String> simpleKafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(this.consumerFactory());
		return factory;
	}
}
