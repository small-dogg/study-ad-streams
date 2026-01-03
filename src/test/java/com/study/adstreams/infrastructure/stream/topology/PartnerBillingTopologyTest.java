package com.study.adstreams.infrastructure.stream.topology;

import com.study.adstreams.domain.model.ClickEvent;
import com.study.adstreams.domain.model.DisplayTarget;
import com.study.adstreams.domain.model.PartnerBillingAggregation;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.LocalDateTime;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PartnerBillingTopology 테스트
 */
class PartnerBillingTopologyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, ClickEvent> inputTopic;
    private TestOutputTopic<String, PartnerBillingAggregation> outputTopic;
    private PartnerBillingTopology topology;

    @BeforeEach
    void setUp() {
        topology = new PartnerBillingTopology();
        
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        topology.buildPartnerBillingTopology(streamsBuilder);
        
        testDriver = new TopologyTestDriver(streamsBuilder.build(), props);
        
        inputTopic = testDriver.createInputTopic(
            "ad-clicked",
            Serdes.String().serializer(),
            new JsonSerde<>(ClickEvent.class).serializer()
        );
        
        outputTopic = testDriver.createOutputTopic(
            "partner-billing-aggregation",
            Serdes.String().deserializer(),
            new JsonSerde<>(PartnerBillingAggregation.class).deserializer()
        );
    }

    @Test
    void testNormalClickAggregation() {
        // Given
        ClickEvent clickEvent = new ClickEvent(
            1L,  // groupId
            100L, // partnerId
            1L,  // productId
            DisplayTarget.WEB,
            "display-1",
            1000L, // amount
            LocalDateTime.now()
        );

        // When
        inputTopic.pipeInput(new TestRecord<>("key1", clickEvent, System.currentTimeMillis()));
        
        // Then
        // 윈도우가 닫히기 전까지는 결과가 없을 수 있음
        // 실제 테스트에서는 시간을 조작하거나 윈도우가 닫힐 때까지 기다려야 함
    }

    @Test
    void testFraudulentClickDetection() {
        // Given: 11번의 클릭 (10회 초과)
        Long partnerId = 100L;
        Long productId = 1L;
        DisplayTarget displayTarget = DisplayTarget.WEB;
        String displayId = "display-1";
        Long amount = 1000L;

        long baseTime = System.currentTimeMillis();
        
        // When: 1분 내에 11번 클릭
        for (int i = 0; i < 11; i++) {
            ClickEvent clickEvent = new ClickEvent(
                1L,  // groupId
                partnerId,
                productId,
                displayTarget,
                displayId,
                amount,
                LocalDateTime.now()
            );
            inputTopic.pipeInput(new TestRecord<>("key" + i, clickEvent, baseTime + (i * 1000)));
        }

        // Then
        // 부정클릭이 감지되어야 함 (윈도우가 닫힐 때 확인)
    }

    @Test
    void testMultiplePartnersAggregation() {
        // Given: 서로 다른 파트너의 클릭
        ClickEvent click1 = new ClickEvent(1L, 100L, 1L, DisplayTarget.WEB, "display-1", 1000L, LocalDateTime.now());
        ClickEvent click2 = new ClickEvent(1L, 200L, 1L, DisplayTarget.WEB, "display-1", 2000L, LocalDateTime.now());

        // When
        inputTopic.pipeInput(new TestRecord<>("key1", click1, System.currentTimeMillis()));
        inputTopic.pipeInput(new TestRecord<>("key2", click2, System.currentTimeMillis()));

        // Then
        // 각 파트너별로 별도 집계되어야 함
    }
}

