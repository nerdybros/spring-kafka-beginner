package io.nerdybros.springkafkabeginner.consumer.store;

import io.nerdybros.springkafkabeginner.consumer.domain.MyEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * 10강: JPA DB 핸들링과 Kafka 처리를 하나의 Transaction으로 처리
 */
@Repository
public class EventStore {

    @Autowired
    private EventRepository repository;

    public void save(MyEvent event) {
        this.repository.save(event);
    }

    public MyEvent findEvent(String id) {
        Optional<MyEvent> byId = this.repository.findById(id);
        if (byId.isPresent()) {
            return byId.get();
        }
        else {
            return null;
        }
    }
}
