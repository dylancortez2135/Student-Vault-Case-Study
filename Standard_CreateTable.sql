-- # ==========================================================================
-- # 1. CUSTOM TYPE DEFINITIONS
-- # ==========================================================================

-- # Composite type supporting State-Tracking Nested Array SCD Type 2 Modeling
-- # This enables granular historical snapshots within the Silver (Intermediate) Layer
CREATE TYPE student_stats AS (
    year_level INT,
    semester INT,
    school_year INT,
    subject_number INT,
    code VARCHAR(50),
    descriptive VARCHAR(100),
    equivalent_grade DECIMAL(9, 2),
    units INT,
    remarks VARCHAR(50),
    term_code INT,
    entry_number INT
);

-- # ==========================================================================
-- # 2. AUTHENTICATION & SECURITY LAYER
-- # ==========================================================================

-- # Secure store for hashed credentials and session state tracking
CREATE TABLE stg_student_auth (
    program VARCHAR(100),
    student_number TEXT PRIMARY KEY,
    password TEXT, -- Encrypted via bcrypt
    last_term TEXT  -- Used for delta-extraction logic
);

-- # ==========================================================================
-- # 3. CORE WAREHOUSE LAYERS (MEDALLION ARCHITECTURE)
-- # ==========================================================================

-- # BRONZE LAYER: Immutable Base Records
-- # The permanent "System of Record" for all raw portal extractions
CREATE TABLE base_student_grades (
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
    entry_number INT,
    PRIMARY KEY (student_number, term_code, subject_number, equivalent_grade, entry_number)
);

-- # SILVER LAYER: State-Tracking Intermediate
-- # Implements Nested Array SCD Type 2 to preserve historical academic states
CREATE TABLE int_student_history (
    student_number INT,
    program VARCHAR(50),
    year_level INT,
    semester INT,
    student_stats student_stats[], -- The Nested Array state-tracker
    school_year INT,
    PRIMARY KEY (student_number, year_level, semester)
);

-- # GOLD LAYER: Analytical Fact Table (Semestral)
-- # Optimized for high-speed Honors Assessment and GWA calculations
CREATE TABLE fct_semestral_grades (
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

-- # GOLD LAYER: Analytical Dimension Table (Yearly)
-- # Provides long-term academic snapshots and cumulative performance metrics
CREATE TABLE dim_academic_years (
    student_number INT,
    program VARCHAR(50),
    year_level INT,
    general_weighted_average FLOAT,
    cumulative_average FLOAT,
    total_units INT,
    academic_awards VARCHAR(100),
    school_year_status VARCHAR(50),
    school_year INT,
    PRIMARY KEY (student_number, year_level)
);

-- # ==========================================================================
-- # 4. AUDIT, LOGGING & MONITORING
-- # ==========================================================================

-- # Observability table for session performance and portal latency tracking
CREATE TABLE IF NOT EXISTS audit_logs_session (
    log_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    student_number TEXT,
    user_category TEXT,       -- Tracks 'NEW' vs 'OLD' user ingestion
    portal_latency_sec FLOAT, -- Monitors University Portal responsiveness
    total_session_sec FLOAT,  -- Total end-to-end ETL duration
    rows_processed INT,
    last_term INT,
    status TEXT
);

-- # Error handling and exception logging for ETL pipeline failures
CREATE TABLE IF NOT EXISTS system_logs_errors (
    log_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    student_number TEXT,
    error TEXT
);