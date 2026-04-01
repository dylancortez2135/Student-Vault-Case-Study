-- # ==========================================
-- # Stored Procedure: Data Transformation Pipeline
-- # ==========================================

CREATE OR REPLACE PROCEDURE prc_transform_student_data()
LANGUAGE plpgsql
AS $$
BEGIN

-- # Step 1: Mapping raw staging data to the structured grades table
-- # Logic: Calculates academic level based on the student's first recorded year
INSERT INTO stg_student_grades
WITH first_year AS (
    SELECT student_number, MIN(school_year) AS first_year
    FROM stg_portal_raw
    GROUP BY student_number
)
SELECT 
    fy.student_number, 
    program, 
    CASE 
        WHEN school_year = first_year THEN 1 
        WHEN school_year = first_year + 1 THEN 2
        WHEN school_year = first_year + 2 THEN 3
        WHEN school_year = first_year + 3 THEN 4 
        ELSE NULL 
    END AS academic_level,
    subject_number, code, descriptive, units, final_average, equivalent_grade, remarks, semester, school_year, term_code
FROM first_year fy
JOIN stg_portal_raw sr ON fy.student_number = sr.student_number
ON CONFLICT (student_number, term_code, subject_number) DO NOTHING;

-- # Step 2: Updating student credentials and last term accessed
INSERT INTO stg_student_auth
SELECT * FROM stg_auth_staging
ON CONFLICT (student_number) 
DO UPDATE SET 
    password = EXCLUDED.password, 
    last_term = EXCLUDED.last_term;

-- # Step 3: Generating the Aggregated Metrics (Fact Table)
-- # Logic: Calculates GWA and assigns Honors (President's/Dean's Lister)
INSERT INTO fct_student_metrics
WITH with_aggregations AS (
    SELECT student_number, SUM(units * final_average) AS weighted_grade, SUM(units) AS total_units, term_code
    FROM stg_student_grades
    GROUP BY student_number, term_code
    ORDER BY student_number, term_code
), unchanging_dimensions AS (
    SELECT student_number, MAX(academic_level) AS year_level, MAX(program) AS program, MAX(remarks) AS remarks, MAX(semester) AS semester, MAX(school_year) AS school_year, term_code
    FROM stg_student_grades
    GROUP BY student_number, term_code
    ORDER BY student_number, term_code
), with_average AS (
    SELECT wa.student_number, ud.program, ud.year_level, CAST((weighted_grade/total_units) AS DECIMAL(9,3)) AS general_weighted_average, total_units, wa.term_code, ud.remarks, ud.semester, ud.school_year
    FROM with_aggregations wa
    JOIN unchanging_dimensions ud ON wa.student_number = ud.student_number AND wa.term_code = ud.term_code
)
SELECT 
    student_number, program, year_level, general_weighted_average, total_units, 
    CASE 
        WHEN general_weighted_average BETWEEN 1.00 AND 1.25 THEN 'President''s lister'
        WHEN general_weighted_average BETWEEN 1.26 AND 1.75 THEN 'Dean''s lister'
        ELSE 'None' 
    END AS academic_honors, 
    term_code, remarks, semester, school_year
FROM with_average
ON CONFLICT (student_number, term_code) DO NOTHING;

-- # Step 4: Updating the Global History Table
-- # Logic: Creates an array-based history of all semesters for longitudinal tracking
INSERT INTO dim_student_history
WITH years as (
    SELECT * FROM GENERATE_SERIES(2024, 2025) AS year
), first_years AS (
    SELECT student_number, MIN(school_year) AS first_year
    FROM fct_student_metrics
    GROUP BY student_number
), students_and_years AS (
    SELECT fy.student_number, first_year, year
    FROM years y
    JOIN first_years fy ON fy.first_year <= year
), windowed AS (
    SELECT 
        sat.student_number, sat.program, sat.year_level AS year_level, sat.semester AS current_semester, sat.school_year AS current_school_year, sat.general_weighted_average, 
        ARRAY_REMOVE(
            ARRAY_AGG(
                CASE WHEN sat.school_year IS NOT NULL THEN ROW(
                    sat.year_level,
                    sat.semester,
                    CONCAT(sat.school_year, ' - ', (sat.school_year + 1)),
                    sat.general_weighted_average,
                    sat.total_units,
                    sat.academic_honors,
                    sat.remarks
                )::student_stats END
            ) OVER (PARTITION BY sat.student_number ORDER BY sat.term_code), 
            NULL
        ) AS student_stats, 
        sat.term_code
    FROM fct_student_metrics sat
    JOIN students_and_years say ON sat.student_number = say.student_number AND sat.school_year = say.year
), with_per_academic_level AS (
    SELECT student_number, program, current_school_year, AVG(general_weighted_average) AS general_weighted_average, MAX(student_stats) AS student_stats, year_level
    FROM windowed
    GROUP BY student_number, year_level, program, current_school_year
    ORDER BY student_number, year_level
)
SELECT 
    wp.student_number, wp.program, wp.year_level, wp.current_school_year, wp.general_weighted_average, 
    AVG(general_weighted_average) OVER (PARTITION BY wp.student_number ORDER BY current_school_year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_average, 
    CASE WHEN CARDINALITY(student_stats) % 2 = 1 THEN 'On-going' ELSE 'Finished' END AS semester_status, 
    student_stats,