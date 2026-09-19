package com.application.service_api.service;

import com.application.service_api.config.IngestionProperties;
import com.application.service_api.exception.IngestionException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class PySparkSplitService {
    private final IngestionProperties properties;
    private final ObjectMapper objectMapper;

    public PySparkSplitService(IngestionProperties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    public List<String> split(String dataFileLocation, Path localInputPath, String fileType, String executionId) {
        Path inputPath = localInputPath;
        Path inputDirectory = inputPath.getParent() == null ? Path.of(".") : inputPath.getParent();
        Path outputDirectory = inputDirectory.resolve("split-files").resolve(executionId + "-" + UUID.randomUUID());
        Path splitterScript = resolveSplitterScript();

        List<String> command = List.of(properties.sparkSubmitCommand(), splitterScript.toString(), "--input", localInputPath.toString(), "--output-directory", outputDirectory.toString(), "--chunk-size", String.valueOf(properties.splitChunkSizeLines()), "--file-type", fileType);

        try {
            log.info("Starting PySpark split: executionId={}, input={}, chunkSize={}", executionId, dataFileLocation, properties.splitChunkSizeLines());
           ProcessBuilder processBuilder = new ProcessBuilder(command).redirectErrorStream(true);

            // Configurations
            processBuilder.environment().put("PYSPARK_PYTHON", properties.pysparkPython());
            processBuilder.environment().put("PYSPARK_DRIVER_PYTHON", properties.pysparkPython());
            processBuilder.environment().put("SPARK_HOME", properties.sparkHome());
            // Tell Spark where the Hadoop binaries folder is located
            processBuilder.environment().put("HADOOP_HOME", "C:\\hadoop");
            // Append the hadoop bin directory to existing system PATH environment variable
            String currentPath = processBuilder.environment().getOrDefault("PATH", "");
            processBuilder.environment().put("PATH", currentPath + ";C:\\hadoop\\bin");

            Process process = processBuilder.start();

            List<String> outputLines;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                outputLines = reader.lines().peek(line -> log.info("PySpark: {}", line)).collect(Collectors.toList());
            }

            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new IngestionException("PySpark split failed with exit code " + exitCode);
            }

            String manifest = outputLines.stream().filter(line -> line.trim().startsWith("[")).reduce((first, last) -> last).orElseThrow(() -> new IngestionException("PySpark returned no split-file manifest"));
                List<String> localSplitFiles = objectMapper.readValue(manifest, new TypeReference<>() {});
                List<String> splitFiles = localSplitFiles.stream().map(this::toContainerPath).toList();
            if (splitFiles.isEmpty()) {
                throw new IngestionException("PySpark returned an empty split-file manifest");
            }
            log.info("PySpark split completed: executionId={}, files={}", executionId, splitFiles.size());
            return splitFiles;
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            log.error("PySpark split was interrupted: executionId={}, input={}", executionId, dataFileLocation, exception);
            throw new IngestionException("PySpark split was interrupted", exception);
        } catch (IOException exception) {
            log.error("Unable to start PySpark split: executionId={}, input={}, command={}", executionId, dataFileLocation, command, exception);
            throw new IngestionException("Unable to start PySpark split job. Command: " + String.join(" ", command), exception);
        }
    }

    private Path resolveSplitterScript() {
        Path configuredPath = Path.of(properties.splitterScript());
        if (configuredPath.toFile().isFile()) {
            return configuredPath;
        }

        Path serviceDirectoryPath = Path.of("..", properties.splitterScript());
        if (serviceDirectoryPath.toFile().isFile()) {
            return serviceDirectoryPath;
        }

        throw new IngestionException("PySpark splitter script was not found: " + properties.splitterScript());
    }

    private String toContainerPath(String localPath) {
        String normalizedRoot = Path.of(properties.localDataRoot()).toAbsolutePath().normalize().toString();
        String normalizedPath = Path.of(localPath).toAbsolutePath().normalize().toString();
        if (!normalizedPath.startsWith(normalizedRoot)) {
            return localPath;
        }
        String relativePath = normalizedPath.substring(normalizedRoot.length()).replaceFirst("^[\\\\/]", "").replace('\\', '/');
        return properties.containerDataRoot().replace('\\', '/') + "/" + relativePath;
    }
}