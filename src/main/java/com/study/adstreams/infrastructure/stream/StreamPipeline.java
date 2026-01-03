package com.study.adstreams.infrastructure.stream;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

/**
 * Kafka Streams 파이프라인 구성
 * DDD/EDA 아키텍처의 Infrastructure 계층에서 스트림 처리 로직을 정의
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class StreamPipeline {

    @Value("${spring.kafka.streams.input-topic:input-topic}")
    private String inputTopic;

    @Value("${spring.kafka.streams.output-topic:output-topic}")
    private String outputTopic;

    @Bean
    public KStream<String, Object> buildStream(StreamsBuilder streamsBuilder) {
        log.info("Building Kafka Streams pipeline from topic: {} to topic: {}", inputTopic, outputTopic);

        KStream<String, Object> stream = streamsBuilder.stream(
            inputTopic,
            Consumed.with(org.apache.kafka.common.serialization.Serdes.String(), new JsonSerde<>(Object.class))
        );

        // TODO: State Store 기반 Tumbling Window 집계 로직 구현
        // 1. 메시지 소비
        // 2. State Store를 사용한 Tumbling Window 집계
        // 3. 집계된 데이터를 새 이벤트로 발행

        stream.to(
            outputTopic,
            Produced.with(org.apache.kafka.common.serialization.Serdes.String(), new JsonSerde<>(Object.class))
        );

        return stream;
    }
}

