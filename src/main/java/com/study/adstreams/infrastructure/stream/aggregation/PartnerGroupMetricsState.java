package com.study.adstreams.infrastructure.stream.aggregation;

import lombok.Data;

import java.io.Serializable;

/**
 * 파트너 그룹 지표 집계 상태
 */
@Data
public class PartnerGroupMetricsState implements Serializable {
    private Long partnerId;
    private Long groupId;
    private long impressionCount = 0L;
    private long clickCount = 0L;
    private long fraudulentClickCount = 0L;
    private long fraudulentAmount = 0L;

    public void incrementImpression() {
        impressionCount++;
    }

    public void incrementClick() {
        clickCount++;
    }

    public void addFraudulentClick(long amount) {
        fraudulentClickCount++;
        fraudulentAmount += amount;
    }
}

