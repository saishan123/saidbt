{{
    config(
        materialized='table',
        file_format='parquet',
        table_type='iceberg',
        partition_by=['month'],
        cluster_by=['id'],
        options={
            'external_location': 's3://bucketname/results/',
        }
    )
}}

WITH base AS (
    SELECT
        id,
        name,
        'jan' AS month,
        jan AS revenue
    FROM {{ source('awsdatacatalog','month_src') }}
    UNION ALL
    SELECT
        id,
        name,
        'feb' AS month,
        feb AS revenue
    FROM {{ source('awsdatacatalog','month_src') }}
    UNION ALL
    SELECT
        id,
        name,
        'mar' AS month,
        mar AS revenue
    FROM {{ source('awsdatacatalog','month_src') }}
    UNION ALL
    SELECT
        id,
        name,
        'apr' AS month,
        apr AS revenue
    FROM {{ source('awsdatacatalog','month_src') }}
    UNION ALL
    SELECT
        id,
        name,
        'may' AS month,
        may AS revenue
    FROM {{ source('awsdatacatalog','month_src') }}
    UNION ALL
    SELECT
        id,
        name,
        'jun' AS month,
        jun AS revenue
    FROM {{ source('awsdatacatalog','month_src') }}
    UNION ALL
    SELECT
        id,
        name,
        'jul' AS month,
        jul AS revenue
    FROM {{ source('awsdatacatalog','month_src') }}
    UNION ALL
    SELECT
        id,
        name,
        'aug' AS month,
        aug AS revenue
    FROM {{ source('awsdatacatalog','month_src') }}
    UNION ALL
    SELECT
        id,
        name,
        'sep' AS month,
        sep AS revenue
    FROM {{ source('awsdatacatalog','month_src') }}
    UNION ALL
    SELECT
        id,
        name,
        'oct' AS month,
        oct AS revenue
    FROM {{ source('awsdatacatalog','month_src') }}
    UNION ALL
    SELECT
        id,
        name,
        'nov' AS month,
        nov AS revenue
    FROM {{ source('awsdatacatalog','month_src') }}
    UNION ALL
    SELECT
        id,
        name,
        'dec' AS month,
        dec AS revenue
    FROM {{ source('awsdatacatalog','month_src') }}
),
final AS (
    SELECT
       -- CONCAT(CAST(id AS VARCHAR), '-', month) AS key,
        id,
        name,
        month,
        revenue
    FROM base
)

SELECT *
FROM final