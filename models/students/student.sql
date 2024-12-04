{{ config(
    materialized='table',
    file_format='parquet',
    location='s3://stockdeta/S3DB/',
    partitioned_by=["enrollment_date"]
) }}

SELECT 
    student_id,
    student_name,
    age,
    gender,
    CAST(enrollment_date AS DATE) AS enrollment_date,
    gpa
FROM students