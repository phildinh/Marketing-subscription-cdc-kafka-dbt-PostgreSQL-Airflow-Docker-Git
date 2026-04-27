-- =============================================================
-- dim_plans.sql
-- Plan dimension — most recent state per plan
-- =============================================================

with stg as (

    select * from {{ ref('stg_plans') }}

),

latest as (

    select distinct on (plan_id)
        plan_id,
        name,
        price,
        billing_cycle,
        created_at,
        cdc_op,
        cdc_ts_ms,
        cdc_consumed_at
    from stg
    order by plan_id, cdc_ts_ms desc

),

final as (

    select
        plan_id,
        name,
        price,
        billing_cycle,
        created_at,
        cdc_op,
        cdc_ts_ms,
        cdc_consumed_at
    from latest

)

select * from final