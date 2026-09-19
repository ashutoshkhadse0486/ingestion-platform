package com.application.service_api.exception;

import com.application.service_api.dto.ErrorResponse;
import jakarta.servlet.http.HttpServletRequest;
import java.io.FileNotFoundException;
import java.time.Instant;
import java.util.stream.Collectors;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import lombok.extern.slf4j.Slf4j;

@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidation(MethodArgumentNotValidException exception, HttpServletRequest request) {
        String message = exception.getBindingResult().getFieldErrors().stream().map(error -> error.getField() + ": " + error.getDefaultMessage()).collect(Collectors.joining(", "));
        log.warn("Validation failed for {}: {}", request.getRequestURI(), message);
        return response(HttpStatus.BAD_REQUEST, "Validation failed", message, request.getRequestURI());
    }

    @ExceptionHandler(FileNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleFileNotFound(FileNotFoundException exception, HttpServletRequest request) {
        log.warn("Required ingestion file was not found for {}: {}", request.getRequestURI(), exception.getMessage(), exception);
        return response(HttpStatus.NOT_FOUND, "File not found", exception.getMessage(), request.getRequestURI());
    }

    @ExceptionHandler(IngestionException.class)
    public ResponseEntity<ErrorResponse> handleIngestion(IngestionException exception, HttpServletRequest request) {
        log.error("Ingestion processing failed for {}: {}", request.getRequestURI(), exception.getMessage(), exception);
        return response(HttpStatus.BAD_GATEWAY, "Ingestion processing failed", exception.getMessage(), request.getRequestURI());
    }

    @ExceptionHandler(WarehouseException.class)
    public ResponseEntity<ErrorResponse> handleWarehouse(WarehouseException exception, HttpServletRequest request) {
        log.error("Warehouse processing failed for {}: {}", request.getRequestURI(), exception.getMessage(), exception);
        return response(HttpStatus.BAD_GATEWAY, "Warehouse processing failed", exception.getMessage(), request.getRequestURI());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponse> handleBadRequest(IllegalArgumentException exception, HttpServletRequest request) {
        log.warn("Invalid request for {}: {}", request.getRequestURI(), exception.getMessage(), exception);
        return response(HttpStatus.BAD_REQUEST, "Invalid request", exception.getMessage(), request.getRequestURI());
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleUnexpected(Exception exception, HttpServletRequest request) {
        log.error("Unexpected error while processing {}", request.getRequestURI(), exception);
        return response(HttpStatus.INTERNAL_SERVER_ERROR, "Internal server error", "The request could not be completed.", request.getRequestURI());
    }

    private ResponseEntity<ErrorResponse> response(HttpStatus status, String error, String message, String path) {
        return ResponseEntity.status(status).body(new ErrorResponse(Instant.now(), status.value(), error, message, path));
    }
}
