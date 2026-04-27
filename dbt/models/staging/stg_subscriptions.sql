-- =============================================================
-- stg_subscriptions.sql
-- Extracts and cleans subscription CDC events
-- =============================================================

with source as (

    select * from {{ source('raw', 'cdc_events') }}
    where source_table = 'subscriptions'

),

cleaned as (

    select
        -- CDC metadata
        id                                          as cdc_id,
        op                                          as cdc_op,
        ts_ms                                       as cdc_ts_ms,
        consumed_at                                 as cdc_consumed_at,

        -- Extract subscription fields
        (after->>'subscription_id')::int            as subscription_id,
        (after->>'user_id')::int                    as user_id,
        (after->>'plan_id')::int                    as plan_id,
        lower(trim(after->>'status'))::varchar      as status,
        to_timestamp((after->>'started_at')::bigint / 1000000.0) as started_at,
        case
            when after->>'ended_at' is not null
            then to_timestamp((after->>'ended_at')::bigint / 1000000.0)
        end                                          as ended_at

    from source
    where op in ('c', 'u', 'r')

)

select * from cleaned