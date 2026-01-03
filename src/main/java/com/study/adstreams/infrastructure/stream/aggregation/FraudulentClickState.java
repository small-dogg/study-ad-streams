package com.study.adstreams.infrastructure.stream.aggregation;

import lombok.Data;

import java.io.Serializable;

/**
 * 부정클릭 집계 상태 (groupId 단위)
 */
@Data
public class FraudulentClickState implements Serializable {
    private long fraudulentClickCount = 0L;
    private long fraudulentAmount = 0L;

    public void addFraudulentClick(long amount) {
        fraudulentClickCount++;
        fraudulentAmount += amount;
    }

    public void merge(FraudulentClickState other) {
        this.fraudulentClickCount += other.fraudulentClickCount;
        this.fraudulentAmount += other.fraudulentAmount;
    }
}

