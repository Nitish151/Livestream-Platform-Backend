package com.lspb.analytics_service.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ChatEvent {
    private String eventType;
    private String messageId;
    private String streamId;
    private String userId;
    private String username;
    private String message;
    private String timestamp;
    
    // Future-proofing / Optional enrichment fields
    private String ip;
    private String countryCode;
}
