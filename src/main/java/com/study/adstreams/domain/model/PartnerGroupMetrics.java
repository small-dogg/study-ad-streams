package com.study.adstreams.domain.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * 파트너 그룹 단위별 지표
 */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PartnerGroupMetrics {
    @JsonProperty("partnerId")
    private Long partnerId;
    
    @JsonProperty("groupId")
    private Long groupId;
    
    @JsonProperty("impressionCount")
    private Long impressionCount;
    
    @JsonProperty("clickCount")
    private Long clickCount;
    
    @JsonProperty("fraudulentClickCount")
    private Long fraudulentClickCount;  // 부정클릭 수
    
    @JsonProperty("fraudulentAmount")
    private Long fraudulentAmount;  // 부정클릭 금액
    
    @JsonProperty("windowStart")
    private LocalDateTime windowStart;
    
    @JsonProperty("windowEnd")
    private LocalDateTime windowEnd;
}

