-- # ==========================================================================
-- # STORED PROCEDURE: prc_transform_student_data
-- # Description: Orchestrates the 3-tier Medallion transformation logic.
-- # Engineering Pattern: State-Tracking Nested Array SCD Type 2 Modeling.
-- # ==========================================================================

CREATE OR REPLACE PROCEDURE prc_transform_student_data()
LANGUAGE plpgsql
AS $$
BEGIN

-- # --------------------------------------------------------------------------
-- # 1. BRONZE LAYER (base_student_grades)
-- # Logic: The "Historical Store" that detects grade changes (SCD Type 2).
-- # --------------------------------------------------------------------------

INSERT INTO base_student_grades
WITH first_year AS (
    -- Identify student's entry year to normalize academic levels (1st-4th Year)
    SELECT student_number, MIN(school_year) AS first_year
    FROM student_raw_staging
    GROUP BY student_number
), with_year_level AS (
    -- Normalizing levels based on the delta from the enrollment year
    SELECT fy.student_number, program, 
           CASE WHEN school_year = first_year THEN 1 
                WHEN school_year = first_year + 1 THEN 2
                WHEN school_year = first_year + 2 THEN 3
                WHEN school_year = first_year + 3 THEN 4
                ELSE NULL END AS academic_level,
           subject_number, code, descriptive, units, final_average, 
           equivalent_grade, remarks, semester, school_year, term_code, 1 AS entry_number
    FROM first_year fy
    JOIN student_raw_staging sr ON fy.student_number = sr.student_number
), with_max_entry AS (
    -- Finding the latest 'active' record in the Bronze layer to compare against
    SELECT student_number, code, MAX(entry_number) AS max_entry
    FROM base_student_grades
    GROUP BY student_number, code
), with_latest_records AS (
    -- Filtering to keep only the most recent version of each subject
    SELECT b.*
    FROM with_max_entry w
    JOIN base_student_grades b ON w.student_number = b.student_number AND w.code = b.code AND w.max_entry = b.entry_number
), with_new_entries AS (
    -- SCD Type 2 Detection: Identifies where grades have changed since last extraction
    SELECT w.*
    FROM with_year_level w
    JOIN with_latest_records wl ON w.student_number = wl.student_number AND w.code = wl.code 
    AND w.equivalent_grade != wl.equivalent_grade
), final_new_row AS (
    -- Creating a new entry version (+1) for changed records
    SELECT w.student_number, w.program, w.academic_level, w.subject_number, w.code, w.descriptive, w.units, w.final_average, w.equivalent_grade, w.remarks, w.semester, w.school_year, w.term_code, (1 + wm.max_entry) AS latest_entry
    FROM with_new_entries w
    JOIN with_max_entry wm ON w.student_number = wm.student_number AND w.code = wm.code
), staging_data AS (
    -- Handling existing records that remain unchanged
    SELECT w.student_number, w.program, w.academic_level, w.subject_number, w.code, w.descriptive, w.units, w.final_average, w.equivalent_grade,
           w.remarks, w.semester, w.school_year, w.term_code, CASE WHEN w.equivalent_grade = b.equivalent_grade THEN b.entry_number ELSE w.entry_number END AS entry_number
    FROM with_year_level w
    LEFT JOIN with_latest_records b ON w.student_number = b.student_number AND w.code = b.code
)
-- Combine new changes and existing data into the permanent Bronze Store
SELECT * FROM final_new_row
UNION ALL
SELECT w.* FROM staging_data w
WHERE NOT EXISTS (SELECT 1 FROM final_new_row m WHERE w.student_number = m.student_number AND w.code = m.code AND w.term_code = m.term_code)
ON CONFLICT (student_number, term_code, subject_number, equivalent_grade, entry_number) DO NOTHING;

-- # --------------------------------------------------------------------------
-- # 2. SILVER LAYER (int_student_history)
-- # Logic: Consolidates subject-level data into semestral state arrays.
-- # --------------------------------------------------------------------------

INSERT INTO int_student_history
WITH years AS (
    -- Scaffolding to ensure continuity in academic years
    SELECT * FROM GENERATE_SERIES(2022,2026) AS years
), student_first_year AS (
    SELECT student_number, MIN(school_year) AS first_year FROM base_student_grades GROUP BY student_number
), student_years AS (
    SELECT sfy.student_number, sfy.first_year, years FROM years y
    JOIN student_first_year sfy ON years >= first_year
), with_latest AS (
    -- Ensuring only the latest SCD version is used for the current state snapshot
    SELECT student_number, code, MAX(entry_number) AS latest_entry_number FROM base_student_grades GROUP BY student_number, code
), latest_rows AS (
    SELECT u.* FROM base_student_grades u
    JOIN with_latest w ON u.student_number = w.student_number AND u.code = w.code AND u.entry_number = w.latest_entry_number
), windowed AS (
    -- Aggregating subjects into chronologically ordered Nested Arrays (The State History)
    SELECT sr.student_number, sr.academic_level, sr.school_year, sr.semester,
    ARRAY_REMOVE(ARRAY_AGG(CASE WHEN sr.school_year IS NOT NULL THEN ROW(sr.academic_level, sr.semester, sr.school_year, sr.subject_number,
                                                                         sr.code, sr.descriptive, sr.equivalent_grade, sr.units, 
                                                                         sr.remarks, sr.term_code, sr.entry_number)::student_stats END)
                 OVER (PARTITION BY sr.student_number ORDER BY term_code), NULL) AS student_stats
    FROM latest_rows sr
    JOIN student_years sy ON sr.student_number = sy.student_number AND sy.years = sr.school_year
), static AS (
    SELECT MAX(student_number) AS student_number, MAX(program) AS program FROM latest_rows GROUP BY student_number
)
SELECT s.student_number, s.program, w.academic_level AS year_level, w.semester, MAX(student_stats) AS student_stats, w.school_year
FROM windowed w
JOIN static s ON w.student_number = s.student_number
GROUP BY s.student_number, s.program, w.academic_level, w.semester, w.school_year
ON CONFLICT (student_number, year_level, semester) 
DO UPDATE SET student_stats = EXCLUDED.student_stats
WHERE int_student_history.student_stats IS DISTINCT FROM EXCLUDED.student_stats;

-- # --------------------------------------------------------------------------
-- # 3. GOLD LAYER (fct_semestral_grades)
-- # Logic: Automated Honors Verification & Semestral GWA Aggregation.
-- # --------------------------------------------------------------------------

INSERT INTO fct_semestral_grades
WITH partial_unnested AS (
    -- Unpacking Silver arrays back to rows for arithmetic operations
    SELECT student_number, program, year_level, semester, UNNEST(student_stats)::student_stats AS student_stats
    FROM int_student_history
), unnested AS (
    -- Filtering to ensure subjects are calculated within their proper academic context
    SELECT student_number, program, (student_stats::student_stats).*
    FROM partial_unnested
    WHERE (student_stats::student_stats).year_level = year_level AND (student_stats::student_stats).semester = semester
), with_latest AS (
    SELECT student_number, code, MAX(entry_number) AS latest_entry_number FROM unnested GROUP BY student_number, code
), latest_rows AS (
    SELECT u.* FROM unnested u
    JOIN with_latest w ON u.student_number = w.student_number AND u.code = w.code AND u.entry_number = w.latest_entry_number
), with_aggregations AS (
    -- Calculation of Weighted Averages (GWA)
    SELECT student_number, SUM(units * equivalent_grade) AS weighted_grade, SUM(units) AS total_units, term_code
    FROM latest_rows GROUP BY student_number, term_code
), unchanging_dimensions AS (
    -- Preserving structural metadata from the Bronze layer
    SELECT student_number, MAX(academic_level) AS year_level, MAX(program) AS program, MAX(remarks) AS remarks, 
           MAX(semester) AS semester, MAX(school_year) AS school_year, term_code
    FROM base_student_grades GROUP BY student_number, term_code
), with_average AS (
    SELECT wa.student_number, ud.program, ud.year_level, CAST((weighted_grade/total_units) AS DECIMAL(9,3)) AS general_weighted_average, 
           total_units, wa.term_code, ud.remarks, ud.semester, ud.school_year
    FROM with_aggregations wa
    JOIN unchanging_dimensions ud ON wa.student_number = ud.student_number AND wa.term_code = ud.term_code
)
-- Applying Honors Logic: President's and Dean's List automated assessment
SELECT student_number, program, year_level, semester, general_weighted_average, total_units, CASE 
    WHEN general_weighted_average BETWEEN 1.00 AND 1.25 THEN 'President''s lister'
    WHEN general_weighted_average BETWEEN 1.26 AND 1.75 THEN 'Dean''s lister'
    ELSE 'None' END AS academic_honors, term_code, remarks, school_year
FROM with_average
ON CONFLICT (student_number, term_code) 
DO UPDATE SET 
    general_weighted_average = EXCLUDED.general_weighted_average,
    total_units = EXCLUDED.total_units,
    academic_honors = EXCLUDED.academic_honors,
    remarks = EXCLUDED.remarks
WHERE fct_semestral_grades.general_weighted_average IS DISTINCT FROM EXCLUDED.general_weighted_average;

-- # --------------------------------------------------------------------------
-- # 4. GOLD LAYER (dim_academic_years)
-- # Logic: Longitudinal snapshots and Cumulative GWA tracking.
-- # --------------------------------------------------------------------------

INSERT INTO dim_academic_years
WITH per_academic_level AS (
    SELECT MAX(student_number) AS student_number, MAX(program) AS program, MAX(year_level) AS year_level, 
           AVG(general_weighted_average) AS general_weighted_average, SUM(total_units) AS total_units, MAX(school_year) AS school_year
    FROM fct_semestral_grades GROUP BY student_number, year_level
), first_year AS (
    SELECT student_number, MIN(school_year) AS first_school_year FROM base_student_grades GROUP BY student_number
), with_awards AS (
    -- Comparing Sem 1 and Sem 2 to generate concatenated yearly award status
    SELECT COALESCE(s1.student_number, s2.student_number) AS student_number, COALESCE(s1.year_level, s2.year_level) AS year_level, 
           CONCAT(CASE WHEN s1.semester IS NOT NULL THEN s1.academic_honors ELSE 'On-going' END, ' : ', 
                  CASE WHEN s2.semester IS NOT NULL THEN s2.academic_honors ELSE 'On-going' END) AS academic_awards,
           CASE WHEN s2.semester IS NULL THEN 'On-going' ELSE 'Finished' END AS school_year_status
    FROM (SELECT * FROM fct_semestral_grades WHERE semester = 1) s1
    LEFT JOIN (SELECT * FROM fct_semestral_grades WHERE semester = 2) s2 ON s1.student_number = s2.student_number AND s1.year_level = s2.year_level
), with_cumulative AS (
    -- Generating Cumulative GPA using an analytical window function
    SELECT p.student_number, p.program, p.year_level, p.general_weighted_average, 
           AVG(general_weighted_average) OVER (PARTITION BY p.student_number ORDER BY p.school_year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_average,
           p.total_units, w.academic_awards, school_year_status, p.school_year
    FROM per_academic_level p
    JOIN with_awards w ON p.student_number = w.student_number AND p.year_level = w.year_level
)
-- Final assignment of Undergraduate vs Graduate status based on enrollment timeline
SELECT w.*, CASE WHEN school_year = first_school_year THEN 'Undergraduate'
                 WHEN school_year = (first_school_year + 1) THEN 'Undergraduate'
                 WHEN school_year = (first_school_year + 2) THEN 'Undergraduate'
                 WHEN school_year = (first_school_year + 3) THEN 'Graduate' END AS academic_level
FROM with_cumulative w
JOIN first_year f ON w.student_number = f.student_number
ON CONFLICT (student_number, year_level) 
DO UPDATE SET 
    general_weighted_average = EXCLUDED.general_weighted_average,
    cumulative_average = EXCLUDED.cumulative_average,
    total_units = EXCLUDED.total_units,
    academic_awards = EXCLUDED.academic_awards,
    school_year_status = EXCLUDED.school_year_status
WHERE dim_academic_years.general_weighted_average IS DISTINCT FROM EXCLUDED.general_weighted_average
OR dim_academic_years.school_year_status IS DISTINCT FROM EXCLUDED.school_year_status;

END;
$$;