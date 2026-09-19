package com.application.service_api.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ingestion")
public record IngestionProperties(long fileSizeThresholdBytes, int splitChunkSizeLines, String sparkSubmitCommand, String pysparkPython, String sparkHome, String splitterScript, String localDataRoot, String containerDataRoot) {
}