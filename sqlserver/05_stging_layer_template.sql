{{
    config(
        materialized = 'table',
        schema = 'Staging',
        full_refresh = true,
        transient = false,
        
    )
}}

/*
    Staging Model: <TABLE NAME>
    Source: raw.<table>
    Target: Staging.stg_<table>

    Notes:
    - Staging = type casting + trimming + normalization
    - DQ Flags included
    - NO deduplication (Silver handles it)
*/

WITH source AS (
    SELECT *
    FROM {{ source('raw', '<table_name>') }}
),

cleaned AS (
    SELECT
        -- TRIM, UPPER, TRY_CAST, etc.
        -- Business key cleanup
        -- Metadata preserved
    FROM source
),

with_dq_flags AS (
    SELECT
        *,
        -- Add DQ flags ONLY
    FROM cleaned
),

final AS (
    SELECT *
    FROM with_dq_flags
)

SELECT * FROM final;
