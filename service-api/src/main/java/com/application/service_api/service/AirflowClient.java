package com.application.service_api.service;

import com.application.service_api.exception.IngestionException;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class AirflowClient {
    private final WebClient airflowWebClient;
    private final String dagId;

    public AirflowClient(WebClient airflowWebClient, @Value("${airflow.dag-id}") String dagId) {
        this.airflowWebClient = airflowWebClient;
        this.dagId = dagId;
    }

    public void trigger(String executionId, List<String> dataFileLocations, String dataFileType, long expectedRecordCount) {

        Map<String, Object> configuration = Map.of("execution_id", executionId, "data_file_locations", dataFileLocations, "file_type", dataFileType, "expected_record_count", expectedRecordCount);
        Map<String, Object> payload = Map.of("dag_run_id", executionId, "conf", configuration);

        try {
            airflowWebClient.post().uri("/dags/{dagId}/dagRuns", dagId).contentType(MediaType.APPLICATION_JSON).bodyValue(payload).retrieve().toBodilessEntity().block();
        } catch (Exception exception) {
            log.error("Unable to trigger Airflow DAG: dagId={}, executionId={}, fileCount={}", dagId, executionId, dataFileLocations.size(), exception);
            throw new IngestionException("Unable to trigger the Airflow ingestion DAG", exception);
        }
    }
}