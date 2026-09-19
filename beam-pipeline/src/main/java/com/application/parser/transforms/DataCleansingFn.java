package com.application.parser.transforms;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.application.parser.utils.EmployeeRecordUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataCleansingFn extends DoFn<String, String> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataCleansingFn.class);
    
    public static final TupleTag<String> SUCCESS_TAG = new TupleTag<>() {};
    public static final TupleTag<String> ERROR_TAG = new TupleTag<>() {};

    private final String executionId;
    private final String fileType;
    private transient ObjectMapper objectMapper;
    private transient CsvMapper csvMapper;
    private transient long csvRecordNumber;
    private static final CsvSchema CSV_SCHEMA = CsvSchema.builder().addColumn("employee_id").addColumn("first_name").addColumn("last_name").addColumn("email").addColumn("phone_number").addColumn("hire_date").addColumn("department").addColumn("job_title").addColumn("salary").addColumn("currency").addColumn("employment_status").addColumn("manager_id").addColumn("is_active").addColumn("skills").addColumn("address_street").addColumn("address_city").addColumn("address_state").addColumn("address_postal_code").addColumn("address_country").addColumn("emergency_contact_name").addColumn("emergency_contact_relationship").addColumn("emergency_contact_phone").addColumn("emergency_contact_email").build();

    public DataCleansingFn(String executionId, String fileType) {
        this.executionId = executionId;
        this.fileType = fileType;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        String rawRecord = context.element();
        
        try {
            if (rawRecord == null || rawRecord.trim().isEmpty()) {
                throw new IllegalArgumentException("Record is completely empty or null.");
            }

            if ("CSV".equalsIgnoreCase(fileType)) {
                LOGGER.info("Processing CSV input: executionId={}", executionId);
                processCsv(rawRecord, context);
            } else {
                LOGGER.info("Processing JSON input: executionId={}", executionId);
                processJson(rawRecord, context);
            }
        } catch (Exception exception) {
            LOGGER.warn("Record failed validation: executionId={}, reason={}", executionId, exception.getMessage());
            writeError(context, rawRecord, exception);
        }
    }

    private void processJson(String jsonText, ProcessContext context) throws Exception {
        try (JsonParser parser = getObjectMapper().getFactory().createParser(jsonText)) {
            JsonNode document = getObjectMapper().readTree(parser);

            if (document.isArray()) {
                LOGGER.info("Processing JSON array: executionId={}, recordCount={}", executionId, document.size());
                for (JsonNode employee : document) {
                    processJsonEmployee(employee, context);
                }
            } else {
                LOGGER.info("Processing JSON record stream: executionId={}", executionId);
                processJsonEmployee(document, context);
                while (parser.nextToken() != null) {
                    processJsonEmployee(getObjectMapper().readTree(parser), context);
                }
            }
        }
    }

    private void processJsonEmployee(JsonNode employee, ProcessContext context) {
        try {
            if (!employee.isObject()) {
                throw new IllegalArgumentException("Each JSON employee record must be an object");
            }

            Map<String, Object> data = getObjectMapper().convertValue(employee, new TypeReference<Map<String, Object>>() {});
            outputCleanRecord(data, context);
        } catch (Exception exception) {
            writeError(context, employee.toString(), exception);
        }
    }

    private void writeError(ProcessContext context, String rawRecord, Exception exception) {
        String errorLog = String.format("ExecutionId: %s | Error: %s | RawData: %s", executionId, exception.getMessage(), rawRecord);
        context.output(ERROR_TAG, errorLog);
        LOGGER.error("Record written to error output: executionId={}, reason={}", executionId, exception.getMessage(), exception);
    }

    private void processCsv(String csvLine, ProcessContext context) {
        csvRecordNumber++;

        if (csvRecordNumber == 1 && csvLine.trim().startsWith("employee_id,")) {
            LOGGER.warn("Skipped CSV header row: executionId={}", executionId);
            return;
        }

        try {
            Map<String, String> csvRow = getCsvMapper().readerFor(Map.class).with(CSV_SCHEMA).readValue(csvLine);
            Map<String, Object> data = new HashMap<>(csvRow);
            convertCsvSkills(data);
            convertCsvAddress(data);
            convertCsvEmergencyContact(data);
            outputCleanRecord(data, context);
        } catch (Exception exception) {
            writeError(context, "CSV record " + csvRecordNumber + ": " + csvLine, exception);
        }
    }

    private void outputCleanRecord(Map<String, Object> data, ProcessContext context) throws Exception {
        EmployeeRecordUtils.validateRecord(data);
        data.put("ingestion_timestamp", Instant.now().toString());
        data.put("execution_id", executionId);
        data.put("source_creation_time", Instant.now().toString());
        EmployeeRecordUtils.encodeSensitiveFields(data);
        context.output(SUCCESS_TAG, getObjectMapper().writeValueAsString(data));
        LOGGER.info("Record cleansed successfully: executionId={}, employeeId={}", executionId, data.get("employee_id"));
    }

    private void convertCsvSkills(Map<String, Object> data) {
        String skillsText = (String) data.get("skills");
        if (skillsText == null || skillsText.isBlank()) {
            data.put("skills", new ArrayList<>());
            return;
        }

        List<String> skills = new ArrayList<>();
        for (String skill : skillsText.split(",")) {
            if (!skill.isBlank()) {
                skills.add(skill.trim());
            }
        }
        data.put("skills", skills);
    }

    private void convertCsvAddress(Map<String, Object> data) {
        Map<String, Object> address = new HashMap<>();
        address.put("street", data.remove("address_street"));
        address.put("city", data.remove("address_city"));
        address.put("state", data.remove("address_state"));
        address.put("postal_code", data.remove("address_postal_code"));
        address.put("country", data.remove("address_country"));
        data.put("address", address);
    }

    private void convertCsvEmergencyContact(Map<String, Object> data) {
        Map<String, Object> emergencyContact = new HashMap<>();
        emergencyContact.put("name", data.remove("emergency_contact_name"));
        emergencyContact.put("relationship", data.remove("emergency_contact_relationship"));
        emergencyContact.put("phone", data.remove("emergency_contact_phone"));
        emergencyContact.put("email", data.remove("emergency_contact_email"));
        data.put("emergency_contact", emergencyContact);
    }

    private ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        return objectMapper;
    }

    private CsvMapper getCsvMapper() {
        if (csvMapper == null) {
            csvMapper = new CsvMapper();
        }
        return csvMapper;
    }

}