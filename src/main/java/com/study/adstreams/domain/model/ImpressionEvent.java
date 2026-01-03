package com.study.adstreams.domain.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * 노출(Impression) 이벤트 도메인 모델
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class ImpressionEvent {
    @JsonProperty("groupId")
    private Long groupId;
    
    @JsonProperty("partnerId")
    private Long partnerId;
    
    @JsonProperty("productId")
    private Long productId;
    
    @JsonProperty("displayTarget")
    private DisplayTarget displayTarget;
    
    @JsonProperty("displayId")
    private String displayId;
    
    @JsonProperty("amount")
    private Long amount;
    
    @JsonProperty("timestamp")
    private LocalDateTime timestamp;
}

