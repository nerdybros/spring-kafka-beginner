package io.nerdybros.springkafkabeginner.producer.rest;

import io.nerdybros.springkafkabeginner.producer.SimpleProducer;
import io.nerdybros.springkafkabeginner.producer.SimpleProducerCallback;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("producer")
public class ProducerController {

    @Autowired
    @Qualifier("simpleProducer")
    private SimpleProducer producer;

    @Autowired
    @Qualifier("simpleProducerCallback")
    private SimpleProducerCallback producerCallback;

    @GetMapping("/simple")
    public void simpleProducer() {
        this.producer.sendMessage("123");
    }

    @GetMapping("/simple-callback")
    public void simpleProducerCallback() {
        this.producerCallback.sendMessage("456");
    }
}
