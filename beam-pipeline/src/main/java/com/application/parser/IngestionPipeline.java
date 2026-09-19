package com.application.parser;

import com.application.parser.options.IngestionOptions;
import com.application.parser.transforms.DataCleansingFn;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.TupleTagList;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.sql.Timestamp;
import java.time.Instant;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestionPipeline {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestionPipeline.class);

    public static void main(String[] args) {
        IngestionOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(IngestionOptions.class);
        LOGGER.info("Beam ingestion started: executionId={}, fileType={}, filePath={}", options.getExecutionId(), options.getFileType(), options.getFilePath());
                
        Pipeline pipeline = Pipeline.create(options);

        DataCleansingFn cleansingFn = new DataCleansingFn(options.getExecutionId(), options.getFileType());
        PCollectionTuple processTuple;

        if ("CSV".equalsIgnoreCase(options.getFileType())) {
            PCollection<String> csvLines = pipeline.apply("Read CSV Lines", TextIO.read().from(options.getFilePath()));
            processTuple = csvLines.apply("Cleanse CSV Records", ParDo.of(cleansingFn).withOutputTags(DataCleansingFn.SUCCESS_TAG, TupleTagList.of(DataCleansingFn.ERROR_TAG)));
        } else {
            PCollection<String> jsonDocuments = pipeline.apply("Find JSON File", FileIO.match().filepattern(options.getFilePath())).apply("Read Whole JSON File", FileIO.readMatches()).apply("Convert File To Text", MapElements.into(TypeDescriptors.strings()).via(IngestionPipeline::readWholeFile));
            processTuple = jsonDocuments.apply("Cleanse JSON Records", ParDo.of(cleansingFn).withOutputTags(DataCleansingFn.SUCCESS_TAG, TupleTagList.of(DataCleansingFn.ERROR_TAG)));
        }

        processTuple.get(DataCleansingFn.ERROR_TAG).apply("Write Error Logs", TextIO.write().to("/opt/data/error-record/" + options.getExecutionId() + "/error_records").withSuffix(".txt"));

        processTuple.get(DataCleansingFn.SUCCESS_TAG).apply("Write to PostgreSQL", JdbcIO.<String>write()
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("org.postgresql.Driver", "jdbc:postgresql://postgres:5432/capstone").withUsername("postgres").withPassword(options.getDatabasePassword()))
                        .withStatement("INSERT INTO target_warehouse (employee_id, first_name, last_name, email, phone_number, hire_date, department, job_title, salary, currency, employment_status, manager_id, is_active, skills, address, emergency_contact, raw_payload, ingestion_timestamp, execution_id, source_creation_time) VALUES (?, ?, ?, ?, ?, ?::date, ?, ?, ?, ?, ?, ?, ?::boolean, ?::jsonb, ?::jsonb, ?::jsonb, ?::jsonb, ?, ?, ?)")
                        .withPreparedStatementSetter((element, statement) -> mapToPreparedStatement(element, statement)));

        try {
            pipeline.run().waitUntilFinish();
            LOGGER.info("Beam ingestion completed: executionId={}", options.getExecutionId());
        } catch (RuntimeException exception) {
            LOGGER.error("Beam ingestion failed: executionId={}, fileType={}, filePath={}",
                    options.getExecutionId(), options.getFileType(), options.getFilePath(), exception);
            throw exception;
        }
    }

    private static String readWholeFile(FileIO.ReadableFile file) {
        try {
            return new String(file.readFullyAsBytes(), StandardCharsets.UTF_8);
        } catch (Exception exception) {
            LOGGER.error("Unable to read JSON input file: file={}", file.getMetadata().resourceId(), exception);
            throw new IllegalStateException("Unable to read the input file", exception);
        }
    }

    private static void mapToPreparedStatement(String jsonRecord, PreparedStatement statement) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> data = mapper.readValue(jsonRecord, Map.class);

            setValue(statement, 1, data.get("employee_id"));
            setValue(statement, 2, data.get("first_name"));
            setValue(statement, 3, data.get("last_name"));
            setValue(statement, 4, data.get("email"));
            setValue(statement, 5, data.get("phone_number"));
            setValue(statement, 6, data.get("hire_date"));
            setValue(statement, 7, data.get("department"));
            setValue(statement, 8, data.get("job_title"));
            setValue(statement, 9, data.get("salary"));
            setValue(statement, 10, data.get("currency"));
            setValue(statement, 11, data.get("employment_status"));
            setValue(statement, 12, data.get("manager_id"));
            setValue(statement, 13, data.get("is_active"));
            setJsonValue(statement, 14, mapper, data.get("skills"));
            setJsonValue(statement, 15, mapper, data.get("address"));
            setJsonValue(statement, 16, mapper, data.get("emergency_contact"));
            setJsonValue(statement, 17, mapper, data);
            statement.setTimestamp(18, Timestamp.from(Instant.parse((String) data.get("ingestion_timestamp"))));
            statement.setString(19, (String) data.get("execution_id"));
            statement.setTimestamp(20, Timestamp.from(Instant.parse((String) data.get("source_creation_time"))));
        } catch (JsonProcessingException e) {
            LOGGER.error("Unable to parse warehouse record JSON");
            throw new RuntimeException("Failed to serialize payload to JSON", e);
        } catch (Exception e) {
            LOGGER.error("Unable to bind warehouse record to database statement", e);
            throw new RuntimeException("Failed to set PreparedStatement parameters", e);
        }
    }

    private static void setValue(PreparedStatement statement, int index, Object value) throws Exception {
        if (value == null || " ".equals(value)) {
            statement.setNull(index, Types.VARCHAR);
        } else {
            statement.setObject(index, value);
        }
    }

    private static void setJsonValue(
            PreparedStatement statement, int index, ObjectMapper mapper, Object value) throws Exception {
        if (value == null || " ".equals(value)) {
            statement.setNull(index, Types.OTHER);
        } else {
            statement.setString(index, mapper.writeValueAsString(value));
        }
    }
}