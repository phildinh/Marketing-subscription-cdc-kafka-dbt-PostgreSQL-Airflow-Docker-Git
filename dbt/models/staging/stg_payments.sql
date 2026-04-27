-- =============================================================
-- stg_payments.sql
-- Extracts and cleans payment CDC events
-- Flags payment retries via duplicate subscription_id
-- =============================================================

with source as (

    select * from {{ source('raw', 'cdc_events') }}
    where source_table = 'payments'

),

cleaned as (

    select
        -- CDC metadata
        id                                          as cdc_id,
        op                                          as cdc_op,
        ts_ms                                       as cdc_ts_ms,
        consumed_at                                 as cdc_consumed_at,

        -- Extract payment fields
        (after->>'payment_id')::int                 as payment_id,
        (after->>'subscription_id')::int            as subscription_id,
        (after->>'amount')::decimal(10,2)           as amount,
        lower(trim(after->>'status'))::varchar      as status,
        to_timestamp((after->>'created_at')::bigint / 1000000.0) as created_at

    from source
    where op in ('c', 'u', 'r')

),

-- Flag retries — more than one payment for the same subscription
flagged as (

    select
        *,
        count(*) over (
            partition by subscription_id
        ) > 1                                       as is_retry

    from cleaned

)

select * from flagged