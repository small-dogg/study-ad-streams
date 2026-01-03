package com.study.adstreams.domain.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * 노출 지면별 지표
 */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DisplayMetrics {
    @JsonProperty("partnerId")
    private Long partnerId;
    
    @JsonProperty("groupId")
    private Long groupId;
    
    @JsonProperty("displayTarget")
    private DisplayTarget displayTarget;
    
    @JsonProperty("displayId")
    private String displayId;
    
    @JsonProperty("impressionCount")
    private Long impressionCount;
    
    @JsonProperty("clickCount")
    private Long clickCount;
    
    @JsonProperty("windowStart")
    private LocalDateTime windowStart;
    
    @JsonProperty("windowEnd")
    private LocalDateTime windowEnd;
}

