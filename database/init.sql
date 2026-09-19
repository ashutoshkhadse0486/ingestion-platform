-- Employee records loaded by the Beam ingestion pipeline.
CREATE TABLE IF NOT EXISTS target_warehouse (
    id BIGSERIAL PRIMARY KEY,
    employee_id VARCHAR(50) NOT NULL, 
    first_name VARCHAR(15) NOT NULL,
    last_name VARCHAR(15),
    email VARCHAR(30) NOT NULL,
    phone_number VARCHAR(20),
    hire_date DATE NOT NULL,
    department VARCHAR(20),
    job_title VARCHAR(30),
    salary VARCHAR(50),      
    currency CHAR(3) NOT NULL,
    employment_status VARCHAR(13),
    manager_id VARCHAR(36),
    is_active BOOLEAN NOT NULL,
    skills JSONB,
    address JSONB,
    emergency_contact JSONB,
    raw_payload JSONB NOT NULL,
    ingestion_timestamp TIMESTAMPTZ NOT NULL,
    execution_id VARCHAR(36) NOT NULL,
    source_creation_time TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_target_warehouse_execution_id
    ON target_warehouse (execution_id);

-- One row per ingestion execution, tracking the status and counts of records ingested.
CREATE TABLE IF NOT EXISTS ingestion_metadata (
    id BIGSERIAL PRIMARY KEY,
    execution_id VARCHAR(36) NOT NULL UNIQUE,
    source_file_location TEXT NOT NULL,
    file_type VARCHAR(20) NOT NULL,
    expected_record_count BIGINT,
    successful_record_count BIGINT NOT NULL DEFAULT 0,
    status VARCHAR(20) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ
);