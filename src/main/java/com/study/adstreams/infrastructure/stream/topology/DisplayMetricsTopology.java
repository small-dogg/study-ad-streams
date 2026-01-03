package com.study.adstreams.infrastructure.stream.topology;

import com.study.adstreams.domain.model.ClickEvent;
import com.study.adstreams.domain.model.DisplayMetrics;
import com.study.adstreams.domain.model.DisplayTarget;
import com.study.adstreams.domain.model.ImpressionEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.LocalDateTime;

/**
 * 파트너 그룹 노출 지면별 지표 토폴로지
 * - ad-impressed, ad-clicked 토픽 조인
 * - partnerId, groupId, displayTarget, displayId 기준 집계
 * - 노출수, 클릭수 집계
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class DisplayMetricsTopology {

    @Value("${spring.kafka.streams.topics.impressed:ad-impressed}")
    private String impressedTopic;

    @Value("${spring.kafka.streams.topics.clicked:ad-clicked}")
    private String clickedTopic;

    @Value("${spring.kafka.streams.topics.display-metrics:display-metrics}")
    private String outputTopic;

    @Bean("displayMetricsStream")
    public KStream<String, DisplayMetrics> buildDisplayMetricsTopology(StreamsBuilder streamsBuilder) {
        log.info("Building Display Metrics Topology from topics: {}, {} to topic: {}", 
            impressedTopic, clickedTopic, outputTopic);

        // Impression 이벤트 스트림
        KStream<String, ImpressionEvent> impressionStream = streamsBuilder.stream(
            impressedTopic,
            Consumed.with(Serdes.String(), new JsonSerde<>(ImpressionEvent.class))
        );

        // Click 이벤트 스트림
        KStream<String, ClickEvent> clickStream = streamsBuilder.stream(
            clickedTopic,
            Consumed.with(Serdes.String(), new JsonSerde<>(ClickEvent.class))
        );

        // 키 변환: partnerId:groupId:displayTarget:displayId
        KStream<String, ImpressionEvent> keyedImpressionStream = impressionStream
            .selectKey((key, impression) -> 
                impression.getPartnerId() + ":" + 
                impression.getGroupId() + ":" + 
                impression.getDisplayTarget() + ":" + 
                impression.getDisplayId()
            );

        KStream<String, ClickEvent> keyedClickStream = clickStream
            .selectKey((key, click) -> 
                click.getPartnerId() + ":" + 
                click.getGroupId() + ":" + 
                click.getDisplayTarget() + ":" + 
                click.getDisplayId()
            );

        // 실시간 집계 (윈도우 없이)
        // Impression 집계
        KTable<String, Long> impressionCounts = keyedImpressionStream
            .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(ImpressionEvent.class)))
            .count();

        // Click 집계
        KTable<String, Long> clickCounts = keyedClickStream
            .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(ClickEvent.class)))
            .count();

        // 조인하여 결과 생성
        KStream<String, DisplayMetrics> resultStream = impressionCounts
            .toStream()
            .leftJoin(
                clickCounts,
                (impressionCount, clickCount) -> new CountValue(impressionCount, clickCount)
            )
            .map((key, counts) -> {
                String[] keyParts = key.split(":");
                Long partnerId = Long.parseLong(keyParts[0]);
                Long groupId = Long.parseLong(keyParts[1]);
                DisplayTarget displayTarget = DisplayTarget.valueOf(keyParts[2]);
                String displayId = keyParts.length > 3 ? keyParts[3] : "";

                DisplayMetrics metrics = DisplayMetrics.builder()
                    .partnerId(partnerId)
                    .groupId(groupId)
                    .displayTarget(displayTarget)
                    .displayId(displayId)
                    .impressionCount(counts.getImpressionCount())
                    .clickCount(counts.getClickCount())
                    .windowStart(LocalDateTime.now())  // 실시간 집계이므로 현재 시간
                    .windowEnd(LocalDateTime.now())
                    .build();

                return KeyValue.pair(key, metrics);
            });

        resultStream.to(
            outputTopic,
            Produced.with(Serdes.String(), new JsonSerde<>(DisplayMetrics.class))
        );

        log.info("Display Metrics Topology configured successfully");
        return resultStream;
    }

    private static class CountValue {
        private final Long impressionCount;
        private final Long clickCount;

        public CountValue(Long impressionCount, Long clickCount) {
            this.impressionCount = impressionCount != null ? impressionCount : 0L;
            this.clickCount = clickCount != null ? clickCount : 0L;
        }

        public Long getImpressionCount() {
            return impressionCount;
        }

        public Long getClickCount() {
            return clickCount;
        }
    }
}
