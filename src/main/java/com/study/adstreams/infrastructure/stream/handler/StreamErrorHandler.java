package com.study.adstreams.infrastructure.stream.handler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Map;

/**
 * Kafka Streams 역직렬화 에러 핸들러
 * 에러 발생 시 로그를 남기고 처리를 계속 진행
 */
@Slf4j
public class StreamErrorHandler implements DeserializationExceptionHandler {

    @Override
    public DeserializationHandlerResponse handle(
            ProcessorContext context,
            org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> record,
            Exception exception) {
        
        log.error("Deserialization error for topic: {}, partition: {}, offset: {}, key: {}",
            record.topic(),
            record.partition(),
            record.offset(),
            record.key(),
            exception);

        // 에러 발생 시 처리를 계속 진행 (CONTINUE)
        // 또는 실패한 레코드를 DLQ로 보낼 수도 있음
        return DeserializationHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // 설정이 필요한 경우 여기에 추가
    }
}

