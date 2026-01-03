package com.study.adstreams.domain.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * 파트너별 과금 집계 결과
 */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PartnerBillingAggregation {
    @JsonProperty("partnerId")
    private Long partnerId;
    
    @JsonProperty("aggregatedAmount")
    private Long aggregatedAmount;  // 정상 클릭 집계 금액
    
    @JsonProperty("fraudulentAmount")
    private Long fraudulentAmount;  // 부정클릭 집계 금액 (10회 초과)
    
    @JsonProperty("totalBillingAmount")
    private Long totalBillingAmount;  // 총 과금액 (aggregatedAmount)
    
    @JsonProperty("windowStart")
    private LocalDateTime windowStart;
    
    @JsonProperty("windowEnd")
    private LocalDateTime windowEnd;
}

