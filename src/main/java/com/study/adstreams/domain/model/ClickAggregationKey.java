package com.study.adstreams.domain.model;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 클릭 집계를 위한 키
 * partnerId, displayTarget, displayId, productId 조합
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class ClickAggregationKey implements Serializable {
    private Long partnerId;
    private DisplayTarget displayTarget;
    private String displayId;
    private Long productId;
    
    public String toKeyString() {
        return partnerId + ":" + displayTarget + ":" + displayId + ":" + productId;
    }
}

