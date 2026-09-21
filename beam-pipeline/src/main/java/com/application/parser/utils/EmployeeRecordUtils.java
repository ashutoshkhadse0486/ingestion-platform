package com.application.parser.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public final class EmployeeRecordUtils {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private EmployeeRecordUtils() {
    }

    /**
     * Validates that required employee fields are present and within acceptable length/range constraints.
     */
    public static void validateRecord(Map<String, Object> record) {
        requireStringLength(record, "employee_id", 7, 7);
        requireStringLength(record, "manager_id", 7, 7);
        requireStringLength(record, "first_name", 3, 15);
        requireStringLength(record, "last_name", 0, 15);
        requireStringLength(record, "email", 13, 30);
        requireStringLength(record, "department", 0, 20);
        requireStringLength(record, "job_title", 0, 30);
        requireStringLength(record, "employment_status", 3, 13);
        requireStringLength(record, "currency", 3, 3);

        String phoneNumber = sanitizePhone(record.get("phone_number"));
        if (phoneNumber.length() != 10) {
            throw new IllegalArgumentException("phone_number must contain exactly 10 digits");
        }
        record.put("phone_number", phoneNumber);

        Map<String, Object> emergencyContact = getMap(record, "emergency_contact");
        String emergencyPhone = sanitizePhone(emergencyContact.get("phone"));
        if (emergencyPhone.length() != 10) {
            throw new IllegalArgumentException("emergency_contact.phone must contain exactly 10 digits");
        }
        emergencyContact.put("phone", emergencyPhone);

        requireStringLength(record, "hire_date", 10, 10);
        try {
            LocalDate.parse(String.valueOf(record.get("hire_date")));
        } catch (DateTimeParseException exception) {
            throw new IllegalArgumentException("hire_date must use YYYY-MM-DD format", exception);
        }
 
        Object active = record.get("is_active");
        if (!(active instanceof Boolean) && !(active instanceof String && (active.equals("true") || active.equals("false")))) {
            throw new IllegalArgumentException("is_active must be true or false");
        }

        Object skillsValue = record.get("skills");
        if (!(skillsValue instanceof List<?> skills)) {
            throw new IllegalArgumentException("skills must be an array of strings");
        }
        for (Object skill : skills) {
            if (!(skill instanceof String)) {
                throw new IllegalArgumentException("skills must contain only strings");
            }
        }
        try {
            if (OBJECT_MAPPER.writeValueAsString(skills).length() > 100) {
                throw new IllegalArgumentException("skills serialized length must not exceed 100 characters");
            }
        } catch (Exception exception) {
            throw new IllegalArgumentException("skills could not be serialized", exception);
        }
    }

    public static void encodeSensitiveFields(Map<String, Object> record) {
        record.put("phone_number", encode(String.valueOf(record.get("phone_number"))));
        record.put("salary", encode(String.valueOf(record.get("salary"))));

        @SuppressWarnings("unchecked")
        Map<String, Object> emergencyContact = (Map<String, Object>) record.get("emergency_contact");
        emergencyContact.put("phone", encode(String.valueOf(emergencyContact.get("phone"))));
    }

    public static String sanitizePhone(Object value) {
        if (value == null) {
            return "";
        }
        String digits = String.valueOf(value).replaceAll("[^0-9]", "");
        return digits.length() > 10 ? digits.substring(digits.length() - 10) : digits;
    }

    private static String encode(String value) {
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getMap(Map<String, Object> record, String fieldName) {
        Object value = record.get(fieldName);
        if (!(value instanceof Map<?, ?>)) {
            throw new IllegalArgumentException(fieldName + " must be an object");
        }
        return (Map<String, Object>) value;
    }

    private static void requireStringLength(Map<String, Object> record, String fieldName, int minimum, int maximum) {
        Object value = record.get(fieldName);
        if (!(value instanceof String text) || text.length() < minimum || text.length() > maximum) {
            throw new IllegalArgumentException(
                    fieldName + " length must be between " + minimum + " and " + maximum + " characters");
        }
    }
}
