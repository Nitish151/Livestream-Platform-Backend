package com.lspb.shared.kafka.schemas;

import java.util.List;
import lombok.Data;

@Data
public class StreamLifecycleEvent {
    private String eventType;
    private String streamId;
    private String userId;
    private String timestamp;
    private String traceId;
    private Metadata metadata;

    @Data
    public static class Metadata {
        private String title;
        private String category;
        private List<String> tags;
    }
}
