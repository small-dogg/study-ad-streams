package com.study.adstreams.infrastructure.stream.topology;

import com.study.adstreams.domain.model.ClickEvent;
import com.study.adstreams.domain.model.ImpressionEvent;
import com.study.adstreams.domain.model.PartnerGroupMetrics;
import com.study.adstreams.infrastructure.stream.aggregation.ClickAggregationState;
import com.study.adstreams.infrastructure.stream.aggregation.FraudulentClickState;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

/**
 * 파트너 그룹 단위별 지표 토폴로지
 * - ad-impressed, ad-clicked 토픽 조인
 * - 5분 Tumbling Window
 * - partnerId, groupId 기준 집계
 * - 노출수, 클릭수, 부정클릭 정보 포함
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class PartnerGroupMetricsTopology {

    @Value("${spring.kafka.streams.topics.impressed:ad-impressed}")
    private String impressedTopic;

    @Value("${spring.kafka.streams.topics.clicked:ad-clicked}")
    private String clickedTopic;

    @Value("${spring.kafka.streams.topics.partner-group-metrics:partner-group-metrics}")
    private String outputTopic;

    @Bean("partnerGroupMetricsStream")
    public KStream<String, PartnerGroupMetrics> buildPartnerGroupMetricsTopology(StreamsBuilder streamsBuilder) {
        log.info("Building Partner Group Metrics Topology from topics: {}, {} to topic: {}", 
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

        // 키 변환: partnerId:groupId
        KStream<String, ImpressionEvent> keyedImpressionStream = impressionStream
            .selectKey((key, impression) -> 
                impression.getPartnerId() + ":" + impression.getGroupId()
            );

        KStream<String, ClickEvent> keyedClickStream = clickStream
            .selectKey((key, click) -> 
                click.getPartnerId() + ":" + click.getGroupId()
            );

        // 5분 Tumbling Window
        TimeWindows tumblingWindow = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5));
        TimeWindows oneMinuteWindow = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1));

        // Impression 집계
        KTable<Windowed<String>, Long> impressionCounts = keyedImpressionStream
            .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(ImpressionEvent.class)))
            .windowedBy(tumblingWindow)
            .count()
            .suppress(
                Suppressed.untilWindowCloses(BufferConfig.unbounded())
                    .withName("suppress-impression-counts")
            );

        // Click 집계 (카운트)
        KTable<Windowed<String>, Long> clickCounts = keyedClickStream
            .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(ClickEvent.class)))
            .windowedBy(tumblingWindow)
            .count()
            .suppress(
                Suppressed.untilWindowCloses(BufferConfig.unbounded())
                    .withName("suppress-click-counts")
            );

        // 부정클릭 집계: 1분 윈도우로 부정클릭 판단 후 5분 윈도우로 재집계
        // 키: partnerId:groupId:displayTarget:displayId:productId
        KTable<Windowed<String>, FraudulentClickState> fraudulentClicks = keyedClickStream
            .selectKey((key, click) -> 
                click.getPartnerId() + ":" + click.getGroupId() + ":" + 
                click.getDisplayTarget() + ":" + click.getDisplayId() + ":" + click.getProductId()
            )
            .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(ClickEvent.class)))
            .windowedBy(oneMinuteWindow)
            .aggregate(
                ClickAggregationState::new,
                (key, clickEvent, state) -> {
                    long amount = clickEvent.getAmount() != null ? clickEvent.getAmount() : 0L;
                    state.addClick(amount);
                    return state;
                },
                Materialized.with(Serdes.String(), new JsonSerde<>(ClickAggregationState.class))
            )
            .suppress(
                Suppressed.untilWindowCloses(BufferConfig.unbounded())
                    .withName("suppress-fraudulent-click-1min")
            )
            .toStream()
            .filter((windowedKey, state) -> state.getFraudulentAmount() > 0)
            .map((windowedKey, state) -> {
                String[] keyParts = windowedKey.key().split(":");
                String groupKey = keyParts[0] + ":" + keyParts[1]; // partnerId:groupId
                FraudulentClickState fraudulentState = new FraudulentClickState();
                fraudulentState.setFraudulentClickCount(1L);
                fraudulentState.setFraudulentAmount(state.getFraudulentAmount());
                return KeyValue.pair(groupKey, fraudulentState);
            })
            .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(FraudulentClickState.class)))
            .windowedBy(tumblingWindow)
            .aggregate(
                FraudulentClickState::new,
                (key, fraudulentState, aggregate) -> {
                    aggregate.merge(fraudulentState);
                    return aggregate;
                },
                Materialized.with(Serdes.String(), new JsonSerde<>(FraudulentClickState.class))
            )
            .suppress(
                Suppressed.untilWindowCloses(BufferConfig.unbounded())
                    .withName("suppress-fraudulent-click-5min")
            );

        // 윈도우 정보를 키에 포함시켜 스트림으로 변환 후 조인
        KStream<String, Map<String, Object>> impressionDataStream = impressionCounts
            .toStream()
            .map((windowedKey, impressionCount) -> {
                String keyWithWindow = windowedKey.key() + ":" + windowedKey.window().start();
                Map<String, Object> data = new HashMap<>();
                data.put("impression", impressionCount);
                data.put("key", windowedKey.key());
                data.put("windowStart", windowedKey.window().start());
                return KeyValue.pair(keyWithWindow, data);
            });

        KStream<String, Map<String, Object>> clickDataStream = clickCounts
            .toStream()
            .map((windowedKey, clickCount) -> {
                String keyWithWindow = windowedKey.key() + ":" + windowedKey.window().start();
                Map<String, Object> data = new HashMap<>();
                data.put("click", clickCount);
                return KeyValue.pair(keyWithWindow, data);
            });

        KStream<String, PartnerGroupMetrics> resultStream = impressionDataStream
            .leftJoin(
                clickDataStream,
                (impressionData, clickData) -> {
                    Map<String, Object> merged = new HashMap<>();
                    if (impressionData != null) {
                        merged.putAll(impressionData);
                    }
                    if (clickData != null && clickData.containsKey("click")) {
                        merged.put("click", clickData.get("click"));
                    } else {
                        merged.put("click", 0L);
                    }
                    return merged;
                },
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5))
            )
            .leftJoin(
                fraudulentClicks.toStream().map((windowedKey, fraudulentState) -> {
                    String keyWithWindow = windowedKey.key() + ":" + windowedKey.window().start();
                    return KeyValue.pair(keyWithWindow, fraudulentState);
                }),
                (metricsData, fraudulentState) -> {
                    Map<String, Object> result = metricsData != null ? new HashMap<>(metricsData) : new HashMap<>();
                    result.put("fraudulentClickCount", fraudulentState != null ? fraudulentState.getFraudulentClickCount() : 0L);
                    result.put("fraudulentAmount", fraudulentState != null ? fraudulentState.getFraudulentAmount() : 0L);
                    return result;
                },
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5))
            )
            .map((keyWithWindow, data) -> {
                String[] parts = keyWithWindow.split(":");
                Long partnerId = Long.parseLong(parts[0]);
                Long groupId = Long.parseLong(parts[1]);
                long windowStart = Long.parseLong(parts[2]);

                LocalDateTime windowStartTime = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(windowStart),
                    ZoneId.systemDefault()
                );
                LocalDateTime windowEndTime = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(windowStart + Duration.ofMinutes(5).toMillis()),
                    ZoneId.systemDefault()
                );

                Long impCount = data.get("impression") != null ? ((Number) data.get("impression")).longValue() : 0L;
                Long clkCount = data.get("click") != null ? ((Number) data.get("click")).longValue() : 0L;
                Long fraudulentClickCount = data.get("fraudulentClickCount") != null ? ((Number) data.get("fraudulentClickCount")).longValue() : 0L;
                Long fraudulentAmount = data.get("fraudulentAmount") != null ? ((Number) data.get("fraudulentAmount")).longValue() : 0L;

                PartnerGroupMetrics metrics = PartnerGroupMetrics.builder()
                    .partnerId(partnerId)
                    .groupId(groupId)
                    .impressionCount(impCount)
                    .clickCount(clkCount)
                    .fraudulentClickCount(fraudulentClickCount)
                    .fraudulentAmount(fraudulentAmount)
                    .windowStart(windowStartTime)
                    .windowEnd(windowEndTime)
                    .build();

                return KeyValue.pair(partnerId + ":" + groupId, metrics);
            });

        resultStream.to(
            outputTopic,
            Produced.with(Serdes.String(), new JsonSerde<>(PartnerGroupMetrics.class))
        );

        log.info("Partner Group Metrics Topology configured successfully");
        return resultStream;
    }
}
