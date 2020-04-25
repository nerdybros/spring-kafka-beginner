package io.nerdybros.springkafkabeginner.consumer.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * 10강: JPA DB 핸들링과 Kafka 처리를 하나의 Transaction으로 처리
 */
@Getter
@Setter
@NoArgsConstructor
@Entity
@Table(name = "processed_event")
public class MyEvent {

    @Id
    private String id;
    private String content;
}
