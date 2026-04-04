package com.lspb.transcoding.model;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class TranscodingJob {
    private String eventType;
    private String streamId;
    private String userId;
    private String timestamp;
    private String traceId;
    private String jobId;
    private String rtmpUrl;
    @JsonAlias("startedAt")
    private String requestedAt;
}
