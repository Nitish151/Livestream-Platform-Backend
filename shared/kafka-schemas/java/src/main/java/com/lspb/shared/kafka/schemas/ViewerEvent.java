package com.lspb.shared.kafka.schemas;

import lombok.Data;

@Data
public class ViewerEvent {
    private String eventType;
    private String streamId;
    private String userId;
    private String timestamp;
    private String traceId;
    private String sessionId;
    private String ip;
    private String countryCode;
    private String region;
    private String userAgent;
    private String rendition;
    private Integer watchDurationSeconds;
}
