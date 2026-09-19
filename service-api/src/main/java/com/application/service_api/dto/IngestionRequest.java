package com.application.service_api.dto;

import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class IngestionRequest {
    private static final String SAFE_LOCATION_PATTERN = "^(?!.*(?:^|[\\\\/])\\.\\.(?:[\\\\/]|$))[a-zA-Z0-9_./\\\\: -]+$";

    @NotBlank(message = "Control file location is required")
    @Pattern(regexp = SAFE_LOCATION_PATTERN, message = "Control file location contains unsupported characters")
    private String controlFileLocation;
}