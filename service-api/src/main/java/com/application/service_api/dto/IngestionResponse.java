package com.application.service_api.dto;

import java.time.Instant;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class IngestionResponse {
    private String executionId;
    private String status;
    private Instant timestamp;
    private String summary;
}