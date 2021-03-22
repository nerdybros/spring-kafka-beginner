package io.nerdybros.springkafkabeginner;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.nerdybros.springkafkabeginner.producer.LessonOneSimpleProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@EmbeddedKafka
@ExtendWith(SpringExtension.class)
@Import(SpringKafkaBeginnerApplicationTests.KafkaTestContainersConfiguration.class)
@ContextConfiguration(classes = {SpringKafkaBeginnerApplicationTests.KafkaTestContainersConfiguration.class})
class SpringKafkaBeginnerApplicationTests {

	private static String topic = "sample-topic";

	@Autowired
	@Qualifier("lessonOneSimpleProducer")
	private LessonOneSimpleProducer producer;

	private KafkaMessageListenerContainer<String, String> container;
	private BlockingQueue<ConsumerRecord<String, String>> consumerRecords;

	@Autowired
	private EmbeddedKafkaBroker embeddedKafka;

	@BeforeEach
	public void setup() {
		consumerRecords = new LinkedBlockingQueue<>();

		Map<String, Object> consumerProperties = KafkaTestUtils.consumerProps("test-group-id", "false", embeddedKafka);
		DefaultKafkaConsumerFactory<String, String> consumer = new DefaultKafkaConsumerFactory<>(consumerProperties);

		ContainerProperties containerProperties = new ContainerProperties(topic);
		container = new KafkaMessageListenerContainer<>(consumer, containerProperties);
		container.setupMessageListener((MessageListener<String, String>) record -> {
			System.out.println("Listened message: " + record.toString());
			consumerRecords.add(record);
		});
		container.start();
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
	}

	@AfterEach
	public void after() {
		container.stop();
	}

	@Test
	public void sendEventTest() throws InterruptedException, JsonProcessingException {
		String payload = "message to send";
		producer.sendMessage(payload);

//		Map<String, Object> configs = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafka));
//		DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(configs, new StringSerializer(), new StringSerializer());
//		Producer<String, String> embeddedProducer = producerFactory.createProducer();
//		embeddedProducer.send(new ProducerRecord<>(topic, null, payload));

		ConsumerRecord<String, String> consumerRecord = consumerRecords.poll(5, TimeUnit.SECONDS);

		assertThat(consumerRecord.key()).isNull();
		assertThat(consumerRecord.value()).isEqualTo(payload);
	}

	@TestConfiguration
	@EmbeddedKafka
	@ExtendWith(SpringExtension.class)
	@ComponentScan(basePackages = {"io.nerdybros.springkafkabeginner"})
	static class KafkaTestContainersConfiguration {

		@Autowired
		EmbeddedKafkaBroker embeddedKafkaBroker;

//		@Bean
//		ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory() {
//			ConcurrentKafkaListenerContainerFactory<Integer, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
//			factory.setConsumerFactory(consumerFactory());
//			return factory;
//		}
//
//		@Bean
//		public ConsumerFactory<Integer, String> consumerFactory() {
//			return new DefaultKafkaConsumerFactory<>(consumerConfigs());
//		}

//		@Bean
//		public Map<String, Object> consumerConfigs() {
//			Map<String, Object> props = new HashMap<>();
//			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
//			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//			props.put(ConsumerConfig.GROUP_ID_CONFIG, "baeldung");
//			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//			return props;
//		}


		/**
		 * Override specific KafkaTemplate bean to test against
		 * @return
		 */
		@Bean("simpleProducerKafkaTemplate")
		@Primary
		public KafkaTemplate<String, String> kafkaTemplate() {
			Map<String, Object> configProps = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafkaBroker));
			DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(configProps, new StringSerializer(), new StringSerializer());
			return new KafkaTemplate<>(producerFactory);
		}
	}
}


