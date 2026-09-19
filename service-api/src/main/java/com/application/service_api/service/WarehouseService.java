package com.application.service_api.service;

import com.application.service_api.dto.WarehouseRecordResponse;
import com.application.service_api.repository.WarehouseRepository;
import java.util.List;
import org.springframework.stereotype.Service;

@Service
public class WarehouseService {
    private final WarehouseRepository warehouseRepository;

    public WarehouseService(WarehouseRepository warehouseRepository) {
        this.warehouseRepository = warehouseRepository;
    }

    public List<WarehouseRecordResponse> findRecords() {
        return warehouseRepository.findAll();
    }
}