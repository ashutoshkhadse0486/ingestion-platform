package com.application.service_api.controller;

import com.application.service_api.dto.WarehouseRecordResponse;
import com.application.service_api.service.WarehouseService;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/warehouse-records")
@RequiredArgsConstructor
public class WarehouseController {
    private final WarehouseService warehouseService;

    @GetMapping
    public ResponseEntity<List<WarehouseRecordResponse>> getRecords() {
        return ResponseEntity.ok(warehouseService.findRecords());
    }
}