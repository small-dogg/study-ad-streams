package com.study.adstreams.infrastructure.messaging;

import com.study.adstreams.domain.event.DomainEvent;

/**
 * 이벤트 발행 인터페이스
 * Infrastructure 레이어에서 이벤트를 Kafka에 발행하는 책임
 */
public interface EventPublisher {
    void publish(DomainEvent event);
}

