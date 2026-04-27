-- =============================================================
-- stg_plans.sql
-- Extracts and cleans plan CDC events from raw.cdc_events
-- =============================================================

with source as (

    select * from {{ source('raw', 'cdc_events') }}
    where source_table = 'plans'

),

cleaned as (

    select
        -- CDC metadata
        id                                          as cdc_id,
        op                                          as cdc_op,
        ts_ms                                       as cdc_ts_ms,
        consumed_at                                 as cdc_consumed_at,

        -- Extract plan fields from the after JSONB column
        (after->>'plan_id')::int                    as plan_id,
        (after->>'name')::varchar                   as name,
        (after->>'price')::decimal(10,2)            as price,
        (after->>'billing_cycle')::varchar          as billing_cycle,
        to_timestamp((after->>'created_at')::bigint / 1000000.0) as created_at

    from source
    where op in ('c', 'u', 'r')

)

select * from cleaned