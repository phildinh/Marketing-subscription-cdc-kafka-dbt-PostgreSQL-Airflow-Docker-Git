-- =============================================================
-- stg_events.sql
-- Extracts and cleans clickstream CDC events
-- Flattens the properties JSONB column into typed columns
-- =============================================================

with source as (

    select * from {{ source('raw', 'cdc_events') }}
    where source_table = 'events'

),

cleaned as (

    select
        -- CDC metadata
        id                                              as cdc_id,
        op                                              as cdc_op,
        ts_ms                                           as cdc_ts_ms,
        consumed_at                                     as cdc_consumed_at,

        -- Extract event fields
        (after->>'event_id')::int                       as event_id,
        (after->>'user_id')::int                        as user_id,
        (after->>'event_type')::varchar                 as event_type,
        to_timestamp((after->>'created_at')::bigint / 1000000.0) as created_at,

        -- Flatten properties JSONB into typed columns
        after->'properties'                             as properties,
        (after->'properties'->>'page')::varchar         as page,
        (after->'properties'->>'referrer')::varchar     as referrer,
        (after->'properties'->>'button')::varchar       as button,
        (after->'properties'->>'feature')::varchar      as feature,
        (after->'properties'->>'plan')::varchar         as plan,
        (after->'properties'->>'device')::varchar       as device,
        (after->'properties'->>'method')::varchar       as method,
        (after->'properties'->>'duration_seconds')::int as duration_seconds

    from source
    where op in ('c', 'r')  -- r = Debezium snapshot read

)

select * from cleaned