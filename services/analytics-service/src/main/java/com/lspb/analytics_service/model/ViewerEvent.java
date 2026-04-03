package com.lspb.analytics_service.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ViewerEvent {
    private String eventType;
    private String streamId;
    private String userId;
    private String timestamp;
    private String traceId;
    private String sessionId;
    private String ip;
    private String userAgent;
    private String rendition;
    private Integer watchDurationSeconds;

    // Enriched fields
    private String countryCode;
    private String region;
}
