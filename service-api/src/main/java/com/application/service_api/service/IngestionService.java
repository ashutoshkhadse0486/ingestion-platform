package com.application.service_api.service;

import com.application.service_api.config.IngestionProperties;
import com.application.service_api.dto.IngestionRequest;
import com.application.service_api.dto.IngestionResponse;
import com.application.service_api.exception.IngestionException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Properties;
import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class IngestionService {
    private final AirflowClient airflowClient;
    private final PySparkSplitService pySparkSplitService;
    private final IngestionProperties ingestionProperties;

    public IngestionService(AirflowClient airflowClient, PySparkSplitService pySparkSplitService, IngestionProperties ingestionProperties) {
        this.airflowClient = airflowClient;
        this.pySparkSplitService = pySparkSplitService;
        this.ingestionProperties = ingestionProperties;
    }

    /**
     * Validates the request, resolves the control file, and delegates to Airflow.
     */
    public IngestionResponse triggerIngestion(IngestionRequest request) {
        validateRequest(request);

        String executionId = UUID.randomUUID().toString();
        ControlFileConfiguration controlFile = readControlFile(resolveLocalPath(request.getControlFileLocation()));
        List<String> dataFileLocations = resolveDataFiles(controlFile.dataFileLocation(), controlFile.fileType(), executionId);

        airflowClient.trigger(executionId, dataFileLocations, controlFile.fileType(), controlFile.expectedRecordCount());
        log.info("Triggered ingestion execution {} for {} file(s)", executionId, dataFileLocations.size());

        return IngestionResponse.builder().executionId(executionId).status("ACCEPTED").timestamp(Instant.now()).summary("Ingestion accepted for Airflow processing").build();
    }

    /**
     * Splits large files before scheduling the Airflow DAG when the dataset exceeds the configured threshold.
     */
    private List<String> resolveDataFiles(String dataFileLocation, String fileType, String executionId) {
        try {
            Path localDataFile = resolveLocalPath(dataFileLocation);
            long fileSize = Files.size(localDataFile);
            log.info("Input file size: executionId={}, bytes={}, threshold={}", executionId, fileSize, ingestionProperties.fileSizeThresholdBytes());
            if (fileSize > ingestionProperties.fileSizeThresholdBytes()) {
                log.warn("Input file exceeds local processing threshold; falling back to PySpark split: executionId={}, bytes={}, threshold={}", executionId, fileSize, ingestionProperties.fileSizeThresholdBytes());
                return pySparkSplitService.split(dataFileLocation, localDataFile, fileType, executionId);
            }
            return List.of(dataFileLocation);
        } catch (IOException exception) {
            log.error("Unable to determine input file size: executionId={}, dataFileLocation={}", executionId, dataFileLocation, exception);
            throw new IngestionException("Unable to determine input file size", exception);
        }
    }

    private Path resolveLocalPath(String containerPath) {
        String containerRoot = ingestionProperties.containerDataRoot().replace('\\', '/');
        String normalizedPath = containerPath.replace('\\', '/');
        if (normalizedPath.equals(containerRoot) || normalizedPath.startsWith(containerRoot + "/")) {
            String relativePath = normalizedPath.substring(containerRoot.length()).replaceFirst("^/", "");
            return Path.of(ingestionProperties.localDataRoot(), relativePath);
        }
        return Path.of(containerPath);
    }

    private ControlFileConfiguration readControlFile(Path controlFilePath) {
        Properties properties = new Properties();

        try (var inputStream = Files.newInputStream(controlFilePath)) {
            properties.load(inputStream);
        } catch (IOException | RuntimeException exception) {
            log.error("Unable to read control file: path={}", controlFilePath, exception);
            throw new IngestionException("Unable to read the control file", exception);
        }

        String recordCount = requiredProperty(properties, "record_count", controlFilePath);
        String fileType = requiredProperty(properties, "file_type", controlFilePath);
        String dataFileLocation = requiredProperty(properties, "data_file_location", controlFilePath);
        if (!fileType.matches("[A-Za-z0-9_-]+")) {
            throw new IngestionException("Control file file_type contains unsupported characters");
        }
        if (!dataFileLocation.matches("^(?!.*(?:^|[\\\\/])\\.\\.(?:[\\\\/]|$))[a-zA-Z0-9_./\\\\: -]+$")) {
            throw new IngestionException("Control file data_file_location contains unsupported characters");
        }
        try {
            long expectedRecordCount = Long.parseLong(recordCount.trim());
            if (expectedRecordCount < 0) {
                throw new NumberFormatException("negative record count");
            }
            return new ControlFileConfiguration(expectedRecordCount, fileType, dataFileLocation);
        } catch (NumberFormatException exception) {
            log.error("Invalid control-file record count: path={}, value={}", controlFilePath, recordCount, exception);
            throw new IngestionException("Control file record_count must be a non-negative integer", exception);
        }
    }

    private String requiredProperty(Properties properties, String name, Path controlFilePath) {
        String value = properties.getProperty(name);
        if (value == null || value.isBlank()) {
            throw new IngestionException("Control file must contain a " + name + " property: " + controlFilePath);
        }
        return value.trim();
    }

    private void validateRequest(IngestionRequest request) {
        if (request == null) {
            throw new IngestionException("Ingestion request is required");
        }
    }

    private record ControlFileConfiguration(long expectedRecordCount, String fileType, String dataFileLocation) {
    }
}