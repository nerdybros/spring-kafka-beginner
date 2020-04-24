package io.nerdybros.springkafkabeginner.producer.rest;

import io.nerdybros.springkafkabeginner.producer.SimpleProducer;
import io.nerdybros.springkafkabeginner.producer.SimpleProducerCallback;
import io.nerdybros.springkafkabeginner.producer.SimplePartitionProducer;
import io.nerdybros.springkafkabeginner.producer.SimpleTransactionalProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("producer")
public class ProducerController {

    @Autowired
    @Qualifier("simpleProducer")
    private SimpleProducer producer;

    @Autowired
    @Qualifier("simpleProducerCallback")
    private SimpleProducerCallback producerCallback;

    @Autowired
    @Qualifier("simplePartitionProducer")
    private SimplePartitionProducer partitionProducer;

    @Autowired
    @Qualifier("simpleTransactionalProducer")
    private SimpleTransactionalProducer transactionalProducer;

    @PostMapping("/simple")
    public void sendUsingSimpleProducer(@RequestBody String message) {
        this.producer.sendMessage(message);
    }

    @PostMapping("/simple-callback")
    public void sendUsingSimpleProducerCallback(@RequestBody String message) {
        this.producerCallback.sendMessage(message);
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
