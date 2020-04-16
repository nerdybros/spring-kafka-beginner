package io.nerdybros.springkafkabeginner.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class SimplePartitionConsumer {

    private final String groupId = "test-group-partition";

    @KafkaListener(containerFactory = "simpleListenerContainerFactory", groupId = groupId,
        topicPartitions = @TopicPartition(topic = "sample-topic-mp",
                partitionOffsets = @PartitionOffset(partition = "0", initialOffset = "0"))
    )
    public void listen(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        System.out.println("partition " + partition + " consumer : " + message);
        // handle business
    }

    @KafkaListener(containerFactory = "simpleListenerContainerFactory", groupId = groupId,
            topicPartitions = @TopicPartition(topic = "sample-topic-mp",
                    partitions = {"1", "2"})
    )
    public void listen2(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        System.out.println("partition " + partition + " consumer : " + message);
        // handle business
    }
}
