package io.nerdybros.springkafkabeginner.consumer.store;

import io.nerdybros.springkafkabeginner.consumer.domain.MyEvent;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * 10강: JPA DB 핸들링과 Kafka 처리를 하나의 Transaction으로 처리
 */
public interface EventRepository extends JpaRepository<MyEvent, String> {
}
