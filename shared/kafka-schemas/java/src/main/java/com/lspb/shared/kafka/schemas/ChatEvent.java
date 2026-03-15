package com.lspb.shared.kafka.schemas;

import lombok.Data;

@Data
public class ChatEvent {
    private String eventType;
    private String streamId;
    private String userId;
    private String timestamp;
    private String traceId;
    private String messageId;
    private String sessionId;
    private String message;
}
