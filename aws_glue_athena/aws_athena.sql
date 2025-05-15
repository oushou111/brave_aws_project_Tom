-- This view combines user data from two sources: user_snapshot and a PII table.
-- It standardizes column names and data types.
CREATE OR REPLACE VIEW combined_user_view AS
SELECT
  user_id,
  first_name,
  last_name,
  email,
  phone_number,
  CAST(date_of_birth AS varchar) AS date_of_birth, -- Standardizing date_of_birth to varchar
  gender,
  city,
  preferred_location_id,
  CAST(created_at AS varchar) AS created_at, -- Standardizing created_at to varchar
  age,
  full_name_length,
  NULL AS processed_time, -- Columns from the second source, null for the first
  NULL AS processed_timestamp,
  NULL AS day,
  NULL AS partition_0,
  NULL AS year,
  NULL AS month
FROM user_snapshot

UNION ALL

SELECT
  user_id,
  first_name,
  last_name,
  email,
  phone_number,
  date_of_birth,
  gender,
  city,
  preferred_location_id,
  created_at,
  NULL AS age, -- Columns from the first source, null for the second
  NULL AS full_name_length,
  processed_time,
  processed_timestamp,
  day,
  partition_0,
  year,
  month
FROM pii_e48c35bb53073907a241c47213a93f97;

-- This view combines schedule data from two sources: schedule and a schedule backup table.
-- It standardizes column names and data types, especially for dates and times.
CREATE OR REPLACE VIEW combined_schedule_view AS
SELECT
  session_id,
  CAST(date AS DATE) AS date, -- Standardizing date to DATE type
  CAST(time AS VARCHAR) AS time, -- Standardizing time to VARCHAR
  datetime,
  class_code,
  class_name,
  instructor_id,
  instructor_name,
  loc_id,
  city,
  location_name,
  week_number,
  year,
  month,
  day_of_week,
  is_weekend,
  duration_mins,
  CAST(NULL AS TIMESTAMP) AS release_timestamp, -- Column from the second source, null for the first
  processed_time
FROM peak_fitness_analytics.schedule

UNION ALL

SELECT
  session_id,
  CAST(SUBSTRING(date, 1, 10) AS DATE) AS date, -- Extracting and casting date part
  SUBSTRING(time, 1, 8) AS time, -- Already a string, no need to CAST again
  CAST(REPLACE(datetime, 't', ' ') AS TIMESTAMP) AS datetime, -- Converting string to timestamp
  class_code,
  class_name,
  CAST(instructor_id AS INT) AS instructor_id, -- Casting instructor_id to INT
  instructor_name,
  loc_id,
  city,
  location_name,
  CAST(week_number AS INT) AS week_number, -- Casting week_number to INT
  CAST(year AS INT) AS year, -- Casting year to INT
  CAST(month AS INT) AS month, -- Casting month to INT
  day_of_week,
  is_weekend,
  CAST(duration_mins AS INT) AS duration_mins, -- Casting duration_mins to INT
  CAST(REPLACE(release_timestamp, 't', ' ') AS TIMESTAMP) AS release_timestamp, -- Converting string to timestamp
  processed_time
FROM peak_fitness_analytics.schedule_bd118202c30487a8cfb86f70bdcd8d07;




CREATE TABLE peak_fitness_analytics.dim_instructor
WITH (
  format = 'PARQUET',
  external_location = 's3://peak-fitness-processed-data-0503/athena-results/dim_instructor/',
  write_compression = 'SNAPPY'
) AS
SELECT DISTINCT
  instructor_id,
  instructor_name
FROM peak_fitness_analytics.combined_schedule_view
WHERE instructor_id IS NOT NULL;




-- This table expands class attendance data by unnesting attendee information.
-- Each row represents a unique attendee in a class session, with email and user_id included.

CREATE TABLE peak_fitness_analytics.expanded_class_attendance
WITH (
  format = 'PARQUET',
  external_location = 's3://peak-fitness-processed-data-0503/athena-results/expanded_class_attendance/',
  write_compression = 'SNAPPY'
) AS
SELECT
  ca.attendance_count,              -- Number of attendees in the class
  ca.class_datetime,                -- Timestamp of the class session
  ca.class_name,                    -- Name of the class
  ca.instructor_name,              -- Name of the instructor
  ca.location_id,                   -- Location identifier for the class
  ca.session_id,                    -- Session ID for the class
  ca.processed_time,                -- Time the record was processed
  ca.processed_timestamp,          -- Timestamp when the record was processed
  a.email,                          -- Email of the attendee (unnested)
  a.user_id                         -- User ID of the attendee (unnested)
FROM class_attendance_235954f72ae4b951d4605dde883a789d ca
CROSS JOIN UNNEST(ca.attendees) AS t (a);



-- This table is a dimension table for user information, version 3.
-- It sources data from combined_user_view and performs data type conversions.
CREATE TABLE peak_fitness_analytics.dim_user_v3
WITH (
  format = 'PARQUET',
  external_location = 's3://peak-fitness-processed-data-0503/athena-results/dim_user_v3/',
  write_compression = 'SNAPPY'
) AS
SELECT
  user_id, -- Primary key for the user
  first_name,
  last_name,
  gender,
  email,
  phone_number,
  TRY_CAST(date_of_birth AS DATE) AS date_of_birth, -- Attempt to cast date_of_birth to DATE
  age,
  city,
  preferred_location_id,
  TRY_CAST(created_at AS TIMESTAMP) AS created_at -- Attempt to cast created_at to TIMESTAMP
FROM peak_fitness_analytics.combined_user_view;

-- This table is a dimension table for class session details.
-- It sources data from combined_schedule_view.
CREATE TABLE peak_fitness_analytics.dim_class_session
WITH (
  format = 'PARQUET',
  external_location = 's3://peak-fitness-processed-data-0503/athena-results/dim_class_session/',
  write_compression = 'SNAPPY'
) AS
SELECT
  session_id, -- Primary key for the class session
  class_code,
  class_name,
  instructor_id,
  loc_id AS location_id, -- Foreign key, references location dimension
  datetime AS class_datetime, -- Full date and time of the class
  duration_mins,
  week_number,
  year,
  month,
  day_of_week,
  is_weekend,
  release_timestamp
FROM peak_fitness_analytics.combined_schedule_view;

-- This table is a dimension table for location details.
-- It sources distinct location data from combined_schedule_view.
CREATE TABLE peak_fitness_analytics.dim_location
WITH (
  format = 'PARQUET',
  external_location = 's3://peak-fitness-processed-data-0503/athena-results/dim_location/',
  write_compression = 'SNAPPY'
) AS
SELECT DISTINCT
  loc_id AS location_id, -- Primary key for the location
  location_name,
  city
FROM peak_fitness_analytics.combined_schedule_view;



-- This table stores facts about class attendance
-- It sources data from expanded_class_attendance and generates a hashed attendance_id.
-- Note: Schema name peak_fitness_analytics might be missing for the table name, assuming current schema or peak_fitness_analytics.
CREATE TABLE fact_class_attendance
WITH (
  external_location = 's3://peak-fitness-processed-data-0503/athena-results/fact_class_attendance_v2/',
  format = 'PARQUET'
) AS
SELECT
    to_hex(md5(CAST(CONCAT(user_id, session_id) AS varbinary))) AS attendance_id, -- Hashed primary key
    user_id, -- Foreign key, references user dimension
    session_id, -- Foreign key, references class session dimension
    location_id, -- Foreign key, references location dimension
    from_unixtime(CAST(class_datetime AS bigint) / 1000) AS class_datetime, -- Converting Unix timestamp to datetime
    processed_time
 FROM expanded_class_attendance;




-- This table stores event facts, partitioned by year and month.
-- It sources data from an 'events' table and performs data type conversions and cleaning.
CREATE TABLE peak_fitness_analytics.fact_event
WITH (
  format = 'PARQUET',
  external_location = 's3://peak-fitness-processed-data-0503/athena-results/fact_event/',
  partitioned_by = ARRAY['year', 'month'],
  write_compression = 'SNAPPY'
) AS
SELECT
  event_id, -- Primary key for the event
  user_id, -- Foreign key, references user dimension
  session_id, -- Foreign key, references class session dimension (can be null if event is not class related)
  event_type,
  location_id, -- Foreign key, references location dimension
  CAST(regexp_replace(class_datetime, 't', ' ') AS timestamp) AS class_datetime, -- Cleaning and casting to timestamp
  processed_time,
  DATE_PARSE(CONCAT(
    CAST(year AS varchar), '-',
    LPAD(CAST(month AS varchar), 2, '0'), '-',
    LPAD(CAST(day AS varchar), 2, '0')
  ), '%Y-%m-%d') AS event_partition_date, -- Creating a date for partitioning
  year, -- Partition key
  month -- Partition key
FROM events
WHERE event_id IS NOT NULL
  AND event_type IS NOT NULL; 