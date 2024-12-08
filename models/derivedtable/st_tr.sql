{{
    config(
        materialized='table',
        file_format='parquet',
        table_type='iceberg',
        cluster_by=['id'],
        options={
            'external_location': 's3://bucketname/results/',
        }
    )
}}

WITH ba AS (
    SELECT
        id,
        name,
        CONCAT(CAST(id AS VARCHAR), '_', name) AS key
    FROM {{ source('awsdatacatalog','month_src') }}
   
)


SELECT *
FROM ba