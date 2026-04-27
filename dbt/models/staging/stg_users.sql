-- =============================================================
-- stg_users.sql
-- Extracts and cleans user CDC events from raw.cdc_events
-- One row per CDC event — not one row per user
-- =============================================================

with source as (

    select * from {{ source('raw', 'cdc_events') }}
    where source_table = 'users'

),

cleaned as (

    select
        -- CDC metadata
        id                                          as cdc_id,
        op                                          as cdc_op,
        ts_ms                                       as cdc_ts_ms,
        consumed_at                                 as cdc_consumed_at,

        -- Extract user fields from the after JSONB column
        (after->>'user_id')::int                    as user_id,
        (after->>'email')::varchar                  as email,
        (after->>'name')::varchar                   as name,
        (after->>'plan_id')::int                    as plan_id,
        to_timestamp((after->>'created_at')::bigint / 1000000.0) as created_at

    from source
    where op in ('c', 'u', 'r')  -- r = Debezium snapshot read

)

select * from cleaned