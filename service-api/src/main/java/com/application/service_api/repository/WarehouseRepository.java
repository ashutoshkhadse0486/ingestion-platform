package com.application.service_api.repository;

import com.application.service_api.dto.WarehouseRecordResponse;
import com.application.service_api.exception.WarehouseException;
import com.application.service_api.util.SensitiveDataDecryptor;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@Slf4j
public class WarehouseRepository {
    private static final String FETCH_SQL = """ 
                                            SELECT id, employee_id, first_name, last_name, email, phone_number, hire_date, department, job_title, salary, currency, employment_status, manager_id, is_active, skills, address, emergency_contact, ingestion_timestamp, execution_id, source_creation_time 
                                            FROM target_warehouse ORDER BY id 
                                            """;

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public WarehouseRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    public List<WarehouseRecordResponse> findAll() {
        try {
            return jdbcTemplate.query(FETCH_SQL, this::mapRecord);
        } catch (RuntimeException exception) {
            log.error("Warehouse fetch failed while reading target_warehouse", exception);
            throw new WarehouseException("Unable to retrieve warehouse records", exception);
        }
    }

    private WarehouseRecordResponse mapRecord(ResultSet resultSet, int rowNumber) throws SQLException {
        try {
            Map<String, Object> emergency = readObject(resultSet.getString("emergency_contact"));
            return new WarehouseRecordResponse(resultSet.getLong("id"), resultSet.getString("employee_id"), resultSet.getString("first_name"), resultSet.getString("last_name"), resultSet.getString("email"), SensitiveDataDecryptor.decrypt(resultSet.getString("phone_number")), resultSet.getObject("hire_date", java.time.LocalDate.class), resultSet.getString("department"), resultSet.getString("job_title"), decryptSalary(resultSet.getString("salary")), resultSet.getString("currency"), resultSet.getString("employment_status"), resultSet.getString("manager_id"), resultSet.getBoolean("is_active"), readList(resultSet.getString("skills")), readObject(resultSet.getString("address")), new WarehouseRecordResponse.EmergencyContact(text(emergency, "name"), text(emergency, "relationship"), SensitiveDataDecryptor.decrypt(text(emergency, "phone")), text(emergency, "email")), resultSet.getTimestamp("ingestion_timestamp").toInstant(), resultSet.getString("execution_id"), resultSet.getTimestamp("source_creation_time").toInstant());
        } catch (Exception exception) {
            throw new SQLException("Unable to map warehouse record at row " + rowNumber, exception);
        }
    }

    private BigDecimal decryptSalary(String encryptedSalary) {
        String salary = SensitiveDataDecryptor.decrypt(encryptedSalary);
        return salary == null || salary.isBlank() ? null : new BigDecimal(salary);
    }

    private List<String> readList(String value) throws Exception {
        return value == null ? null : objectMapper.readValue(value, objectMapper.getTypeFactory().constructCollectionType(List.class, String.class));
    }

    private Map<String, Object> readObject(String value) throws Exception {
        return value == null ? null : objectMapper.readValue(value, objectMapper.getTypeFactory().constructMapType(Map.class, String.class, Object.class));
    }

    private String text(Map<String, Object> object, String field) {
        Object value = object == null ? null : object.get(field);
        return value == null ? null : String.valueOf(value);
    }
}