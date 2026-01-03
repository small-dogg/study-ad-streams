package com.study.adstreams.infrastructure.stream.aggregation;

import lombok.Data;

import java.io.Serializable;

/**
 * 클릭 집계 상태
 */
@Data
public class ClickAggregationState implements Serializable {
    private int clickCount = 0;
    private long normalAmount = 0L;
    private long fraudulentAmount = 0L;

    public void addClick(long amount) {
        clickCount++;
        if (clickCount > 10) {
            fraudulentAmount += amount;
        } else {
            normalAmount += amount;
        }
    }
}

