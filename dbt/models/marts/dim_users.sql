-- =============================================================
-- dim_users.sql
-- User dimension — most recent state per user
-- Deduplicates CDC events keeping only the latest per user_id
-- SCD Type 2 history is handled in the snapshot
-- =============================================================

with stg as (

    select * from {{ ref('stg_users') }}

),

-- Keep only the latest CDC event per user
-- A user may have many update events — we want current state
latest as (

    select distinct on (user_id)
        user_id,
        email,
        name,
        plan_id,
        created_at,
        cdc_op,
        cdc_ts_ms,
        cdc_consumed_at
    from stg
    order by user_id, cdc_ts_ms desc

),

final as (

    select
        user_id,
        email,
        name,
        plan_id,
        created_at,
        cdc_op,
        cdc_ts_ms,
        cdc_consumed_at
    from latest

)

select * from final