package io.nerdybros.springkafkabeginner.producer.rest;

import java.util.List;

import io.nerdybros.springkafkabeginner.producer.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("producer")
public class ProducerController {

	@Autowired
	@Qualifier("lessonOneSimpleProducer")
	private LessonOneSimpleProducer producer;

	@Autowired
	@Qualifier("lessonTwoSimpleProducerCallback")
	private LessonTwoSimpleProducerCallback producerCallback;

	@Autowired
	@Qualifier("lessonFourSimplePartitionProducer")
	private LessonFourSimplePartitionProducer partitionProducer;

	@Autowired
	@Qualifier("simpleTransactionalProducer")
	private SimpleTransactionalProducer transactionalProducer;

	@Autowired
	@Qualifier("lessonTwoSimpleProducerWithCustomProducerListener")
	private LessonTwoSimpleProducerWithCustomProducerListener customProducerListenerProducer;

	@PostMapping("/simple")
	public void sendUsingSimpleProducer(@RequestBody String message) {
		this.producer.sendMessage(message);
	}

	@PostMapping("/simple-callback")
	public void sendUsingSimpleProducerCallback(@RequestBody String message) {
		this.producerCallback.sendMessage(message);
	}

	@PostMapping("/simple-custom-listener")
	public void sendUsingcustomListenerProducer(@RequestBody String message) {
		this.customProducerListenerProducer.sendMessage(message);
	}

	@PostMapping("/simple-partition")
	public void sendUsingSimplePartitionProducer(@RequestParam String key, @RequestBody String message) {
		this.partitionProducer.sendMessage(key, message);
	}

	@PostMapping("/simple-partition-no-key")
	public void sendUsingSimplePartitionNoKey(@RequestBody String message) {
		this.partitionProducer.sendMessageNoMessageKey(message);
	}

	@PostMapping("/simple-transactional-method")
	public void sendUsingSimpleTransactionalProducerMethod(@RequestBody List<String> messages) {
		this.transactionalProducer.sendMessageUsingMethod(messages);
	}

	@PostMapping("/simple-transactional-annotation")
	public void sendUsingSimpleTransactionProducerAnnotation(@RequestBody List<String> messages) {
		this.transactionalProducer.sendMessageUsingAnnotation(messages);
	}
}
