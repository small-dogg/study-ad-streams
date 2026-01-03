package com.study.adstreams.infrastructure.stream.topology;

import com.study.adstreams.domain.model.ClickAggregationKey;
import com.study.adstreams.domain.model.ClickEvent;
import com.study.adstreams.domain.model.PartnerBillingAggregation;
import com.study.adstreams.infrastructure.stream.aggregation.ClickAggregationState;
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

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

/**
 * 파트너별 과금목적 집계 토폴로지
 * - ad-clicked 토픽만 수신
 * - 1분 Tumbling Window
 * - partnerId, displayTarget, displayId, productId 기준 집계
 * - 1분 동안 10번 초과 클릭은 부정클릭으로 처리
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class PartnerBillingTopology {

    @Value("${spring.kafka.streams.topics.clicked:ad-clicked}")
    private String clickedTopic;

    @Value("${spring.kafka.streams.topics.partner-billing:partner-billing-aggregation}")
    private String outputTopic;

    @Bean("partnerBillingStream")
    public KStream<String, PartnerBillingAggregation> buildPartnerBillingTopology(StreamsBuilder streamsBuilder) {
        log.info("Building Partner Billing Topology from topic: {} to topic: {}", clickedTopic, outputTopic);

        // Click 이벤트 스트림 생성
        KStream<String, ClickEvent> clickStream = streamsBuilder.stream(
            clickedTopic,
            Consumed.with(Serdes.String(), new JsonSerde<>(ClickEvent.class))
        );

        // 키 변환: partnerId, displayTarget, displayId, productId 조합
        KStream<ClickAggregationKey, ClickEvent> keyedStream = clickStream
            .selectKey((key, clickEvent) -> new ClickAggregationKey(
                clickEvent.getPartnerId(),
                clickEvent.getDisplayTarget(),
                clickEvent.getDisplayId(),
                clickEvent.getProductId()
            ));

        // 1분 Tumbling Window로 집계 (클릭 수와 금액 추적)
        TimeWindows tumblingWindow = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1));

        KTable<Windowed<ClickAggregationKey>, ClickAggregationState> aggregatedByKey = keyedStream
            .groupByKey(Grouped.with(new JsonSerde<>(ClickAggregationKey.class), new JsonSerde<>(ClickEvent.class)))
            .windowedBy(tumblingWindow)
            .aggregate(
                ClickAggregationState::new,
                (key, clickEvent, state) -> {
                    long amount = clickEvent.getAmount() != null ? clickEvent.getAmount() : 0L;
                    state.addClick(amount);
                    return state;
                },
                Materialized.with(new JsonSerde<>(ClickAggregationKey.class), new JsonSerde<>(ClickAggregationState.class))
            );

        // 파트너별로 재집계 (윈도우 정보 유지)
        KStream<String, PartnerBillingAggregation> resultStream = aggregatedByKey
            .toStream()
            .groupBy(
                (windowedKey, state) -> {
                    // 파트너 ID와 윈도우 시작 시간을 키로 사용
                    long windowStart = windowedKey.window().start();
                    return windowedKey.key().getPartnerId() + ":" + windowStart;
                },
                Grouped.with(Serdes.String(), new JsonSerde<>(ClickAggregationState.class))
            )
            .windowedBy(tumblingWindow)
            .aggregate(
                () -> new PartnerBillingAggregationState(),
                (key, aggregationState, partnerState) -> {
                    // 키에서 partnerId 추출
                    String[] parts = key.split(":");
                    partnerState.setPartnerId(Long.parseLong(parts[0]));
                    partnerState.addNormalAmount(aggregationState.getNormalAmount());
                    partnerState.addFraudulentAmount(aggregationState.getFraudulentAmount());
                    return partnerState;
                },
                Materialized.with(Serdes.String(), new JsonSerde<>(PartnerBillingAggregationState.class))
            )
            .toStream()
            .map((windowedKey, state) -> {
                Windowed<String> windowed = (Windowed<String>) windowedKey;
                String[] keyParts = windowed.key().split(":");
                Long partnerId = Long.parseLong(keyParts[0]);
                
                LocalDateTime windowStart = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(windowed.window().start()),
                    ZoneId.systemDefault()
                );
                LocalDateTime windowEnd = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(windowed.window().end()),
                    ZoneId.systemDefault()
                );

                PartnerBillingAggregation aggregation = PartnerBillingAggregation.builder()
                    .partnerId(partnerId)
                    .aggregatedAmount(state.getAggregatedAmount())
                    .fraudulentAmount(state.getFraudulentAmount())
                    .totalBillingAmount(state.getAggregatedAmount())
                    .windowStart(windowStart)
                    .windowEnd(windowEnd)
                    .build();

                return KeyValue.pair(String.valueOf(partnerId), aggregation);
            });

        resultStream.to(
            outputTopic,
            Produced.with(Serdes.String(), new JsonSerde<>(PartnerBillingAggregation.class))
        );

        log.info("Partner Billing Topology configured successfully");
        return resultStream;
    }

    // 파트너별 집계 상태
    private static class PartnerBillingAggregationState {
        private Long partnerId;
        private long aggregatedAmount = 0L;
        private long fraudulentAmount = 0L;

        public void setPartnerId(Long partnerId) {
            this.partnerId = partnerId;
        }

        public void addNormalAmount(long amount) {
            aggregatedAmount += amount;
        }

        public void addFraudulentAmount(long amount) {
            fraudulentAmount += amount;
        }

        public Long getPartnerId() {
            return partnerId;
        }

        public long getAggregatedAmount() {
            return aggregatedAmount;
        }

        public long getFraudulentAmount() {
            return fraudulentAmount;
        }
    }
}
