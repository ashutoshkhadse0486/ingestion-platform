package com.application.service_api.service;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.application.service_api.config.IngestionProperties;
import com.application.service_api.dto.IngestionRequest;
import com.application.service_api.exception.IngestionException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IngestionServiceTest {

    @TempDir
    Path tempDir;

    private AirflowClient airflowClient;
    private PySparkSplitService pySparkSplitService;
    private IngestionService ingestionService;

    @BeforeEach
    void setUp() {
        airflowClient = mock(AirflowClient.class);
        pySparkSplitService = mock(PySparkSplitService.class);

        IngestionProperties ingestionProperties = new IngestionProperties(1024L, 100, "spark-submit", "/usr/bin/python3", "/opt/spark", "spark-job/split_file.py", tempDir.toString(), "/opt/data");

        ingestionService = new IngestionService(airflowClient, pySparkSplitService, ingestionProperties);
    }

    @Test
    void triggerIngestion_shouldTriggerAirflow_forValidControlFile() throws Exception {
        Path controlFile = tempDir.resolve("control-file/control.properties");
        Files.createDirectories(controlFile.getParent());
        Files.writeString(controlFile, "record_count=10\nfile_type=CSV\ndata_file_location=/opt/data/ingestion-files/test.csv\n");

        Path dataFile = tempDir.resolve("ingestion-files/test.csv");
        Files.createDirectories(dataFile.getParent());
        Files.writeString(dataFile, "employee_id,name\n1,Alice\n");

        IngestionRequest request = new IngestionRequest();
        request.setControlFileLocation("/opt/data/control-file/control.properties");

        ingestionService.triggerIngestion(request);

        verify(airflowClient).trigger(anyString(), eq(List.of("/opt/data/ingestion-files/test.csv")), eq("CSV"), eq(10L));
    }

    @Test
    void triggerIngestion_shouldUseSplitFiles_whenInputExceedsThreshold() throws Exception {
        Path controlFile = tempDir.resolve("control-file/control.properties");
        Files.createDirectories(controlFile.getParent());
        Files.writeString(controlFile, "record_count=200\nfile_type=CSV\ndata_file_location=/opt/data/ingestion-files/large.csv\n");

        Path dataFile = tempDir.resolve("ingestion-files/large.csv");
        Files.createDirectories(dataFile.getParent());
        Files.writeString(dataFile, "employee_id,name\n1,Alice\n2,Bob\n3,Charlie\n");

        when(pySparkSplitService.split(eq("/opt/data/ingestion-files/large.csv"), any(Path.class), eq("CSV"), anyString())).thenReturn(List.of("/opt/data/ingestion-files/part-1.csv", "/opt/data/ingestion-files/part-2.csv"));

        IngestionProperties splitThresholdProperties = new IngestionProperties(1L, 100, "spark-submit", "/usr/bin/python3", "/opt/spark", "spark-job/split_file.py", tempDir.toString(), "/opt/data");
        ingestionService = new IngestionService(airflowClient, pySparkSplitService, splitThresholdProperties);

        IngestionRequest request = new IngestionRequest();
        request.setControlFileLocation("/opt/data/control-file/control.properties");

        ingestionService.triggerIngestion(request);

        verify(pySparkSplitService).split(eq("/opt/data/ingestion-files/large.csv"), any(Path.class), eq("CSV"), anyString());
        verify(airflowClient).trigger(anyString(), eq(List.of("/opt/data/ingestion-files/part-1.csv", "/opt/data/ingestion-files/part-2.csv")), eq("CSV"), eq(200L));
    }

    @Test
    void triggerIngestion_shouldRejectNegativeRecordCounts() throws Exception {
        Path controlFile = tempDir.resolve("control-file/control.properties");
        Files.createDirectories(controlFile.getParent());
        Files.writeString(controlFile, "record_count=-1\nfile_type=CSV\ndata_file_location=/opt/data/ingestion-files/test.csv\n");

        IngestionRequest request = new IngestionRequest();
        request.setControlFileLocation("/opt/data/control-file/control.properties");

        assertThrows(IngestionException.class, () -> ingestionService.triggerIngestion(request));
        verify(airflowClient, never()).trigger(anyString(), any(), anyString(), any(Long.class));
    }

    @Test
    void triggerIngestion_shouldRejectNullRequest() {
        assertThrows(IngestionException.class, () -> ingestionService.triggerIngestion(null));
        verify(airflowClient, never()).trigger(anyString(), any(), anyString(), any(Long.class));
    }
}
