-- # ==========================================
-- # 1. Custom Type Definitions
-- # ==========================================

-- # Composite type used for longitudinal student performance tracking
CREATE TYPE student_stats AS (
    academic_level INT,
    semester INT,
    school_year VARCHAR(50),
    general_weighted_average DECIMAL(9, 2),
    total_units INT,
    academic_honors VARCHAR(50),
    remarks VARCHAR(50)
);

-- # ==========================================
-- # 2. Staging Tables (Raw Landing Zone)
-- # ==========================================

-- # Temporary landing for grades extracted from the portal
CREATE TABLE stg_portal_raw (
    student_number INT,
    program VARCHAR(100),
    subject_number INT,
    code VARCHAR(100),
    descriptive VARCHAR(100),
    units INT,
    final_average FLOAT,
    equivalent_grade FLOAT,
    remarks VARCHAR(100),
    semester INT,
    school_year INT,
    term_code INT
);

-- # Temporary landing for authentication and term tracking
CREATE TABLE stg_auth_staging (
    program VARCHAR(100),
    student_number TEXT,
    password TEXT,
    last_term TEXT
);

-- # ==========================================
-- # 3. Core Warehouse Tables
-- # ==========================================

-- # Staging table for cleaned and structured grade records
CREATE TABLE stg_student_grades (
    student_number INT,
    program VARCHAR(100),
    academic_level INT,
    subject_number INT,
    code VARCHAR(100),
    descriptive VARCHAR(100),
    units INT,
    final_average FLOAT,
    equivalent_grade FLOAT,
    remarks VARCHAR(100),
    semester INT,
    school_year INT,
    term_code INT,
    PRIMARY KEY (student_number, term_code, subject_number)
);

-- # Persistent store for encrypted user credentials
CREATE TABLE stg_student_auth (
    program VARCHAR(100),
    student_number TEXT PRIMARY KEY,
    password TEXT,
    last_term TEXT
);

-- # Fact table containing aggregated academic metrics per term
CREATE TABLE fct_student_metrics (
    student_number INT,
    program VARCHAR(50),
    year_level INT,
    general_weighted_average FLOAT,
    total_units INT,
    academic_honors VARCHAR(50),
    term_code INT,
    remarks VARCHAR(50),
    semester INT,
    school_year INT,
    PRIMARY KEY (student_number, term_code)
);

-- # Dimension table for high-level historical student snapshots
CREATE TABLE dim_student_history (
    student_number INT,
    program VARCHAR(100),
    year_level INT,
    school_year INT,
    general_weighted_average FLOAT,
    cumulative_average FLOAT,
    semester_status VARCHAR(50),
    student_stats student_stats[],
    academic_level VARCHAR(50),
    PRIMARY KEY (student_number, school_year)
);

-- # ==========================================
-- # 4. Audit & Monitoring
-- # ==========================================

-- # Table for tracking system performance and extraction logs
CREATE TABLE IF NOT EXISTS audit_logs_session (
    log_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    student_number TEXT,
    user_category TEXT,
    portal_latency_sec FLOAT,
    total_session_sec FLOAT,
    rows_processed INT,
    last_term INT,
    status TEXT
);

-- # Table for capturing detailed system error messages
CREATE TABLE IF NOT EXISTS system_logs_errors (
    log_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    student_number TEXT,
    error TEXT
);