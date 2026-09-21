package com.application.service_api.controller;

import com.application.service_api.dto.IngestionRequest;
import com.application.service_api.dto.IngestionResponse;
import com.application.service_api.service.IngestionService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/api/v1/ingestions")
@RequiredArgsConstructor
@Slf4j
public class IngestionController {

    private final IngestionService ingestionService;

    /**
     * Accepts a control file path and starts the ingestion workflow asynchronously.
     */
    @PostMapping("/trigger")
    public ResponseEntity<IngestionResponse> triggerIngestion(@Valid @RequestBody IngestionRequest request) {
        log.info("Received ingestion trigger request for control file {}", request.getControlFileLocation());
        return ResponseEntity.accepted().body(ingestionService.triggerIngestion(request));
    }
}