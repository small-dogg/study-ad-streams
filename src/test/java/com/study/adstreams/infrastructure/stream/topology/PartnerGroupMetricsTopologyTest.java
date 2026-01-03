package com.study.adstreams.infrastructure.stream.topology;

import com.study.adstreams.domain.model.ClickEvent;
import com.study.adstreams.domain.model.DisplayTarget;
import com.study.adstreams.domain.model.ImpressionEvent;
import com.study.adstreams.domain.model.PartnerGroupMetrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.LocalDateTime;
import java.util.Properties;

/**
 * PartnerGroupMetricsTopology 테스트
 */
class PartnerGroupMetricsTopologyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, ImpressionEvent> impressionInputTopic;
    private TestInputTopic<String, ClickEvent> clickInputTopic;
    private TestOutputTopic<String, PartnerGroupMetrics> outputTopic;
    private PartnerGroupMetricsTopology topology;

    @BeforeEach
    void setUp() {
        topology = new PartnerGroupMetricsTopology();
        
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        topology.buildPartnerGroupMetricsTopology(streamsBuilder);
        
        testDriver = new TopologyTestDriver(streamsBuilder.build(), props);
        
        impressionInputTopic = testDriver.createInputTopic(
            "ad-impressed",
            Serdes.String().serializer(),
            new JsonSerde<>(ImpressionEvent.class).serializer()
        );
        
        clickInputTopic = testDriver.createInputTopic(
            "ad-clicked",
            Serdes.String().serializer(),
            new JsonSerde<>(ClickEvent.class).serializer()
        );
        
        outputTopic = testDriver.createOutputTopic(
            "partner-group-metrics",
            Serdes.String().deserializer(),
            new JsonSerde<>(PartnerGroupMetrics.class).deserializer()
        );
    }

    @Test
    void testImpressionAndClickJoin() {
        // Given
        Long partnerId = 100L;
        Long groupId = 1L;
        
        ImpressionEvent impression = new ImpressionEvent(
            groupId, partnerId, 1L, DisplayTarget.WEB, "display-1", 1000L, LocalDateTime.now()
        );
        
        ClickEvent click = new ClickEvent(
            groupId, partnerId, 1L, DisplayTarget.WEB, "display-1", 1000L, LocalDateTime.now()
        );

        long timestamp = System.currentTimeMillis();
        
        // When
        impressionInputTopic.pipeInput(new TestRecord<>("key1", impression, timestamp));
        clickInputTopic.pipeInput(new TestRecord<>("key1", click, timestamp));

        // Then
        // 윈도우가 닫힐 때 조인 결과가 출력되어야 함
    }
}

