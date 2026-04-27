-- =============================================================
-- fct_subscriptions.sql
-- Fact table — one row per subscription event
-- Grain: one row per subscription_id + cdc_op + cdc_ts_ms
-- Key metrics: MRR, churn, subscription duration
-- =============================================================

with stg_sub as (

    select * from {{ ref('stg_subscriptions') }}

),

stg_plans as (

    select * from {{ ref('stg_plans') }}

),

-- Get the latest plan price for each plan_id
-- Used to calculate MRR
latest_plans as (

    select distinct on (plan_id)
        plan_id,
        price,
        billing_cycle
    from stg_plans
    order by plan_id, cdc_ts_ms desc

),

final as (

    select
        -- Keys
        s.subscription_id,
        s.user_id,
        s.plan_id,

        -- Subscription state
        s.status,
        s.started_at,
        s.ended_at,

        -- MRR calculation
        -- Annual plans divided by 12 to get monthly equivalent
        case
            when p.billing_cycle = 'annual'
            then round(p.price / 12, 2)
            else p.price
        end                                         as mrr,

        -- Churn flag — subscription was cancelled
        case
            when s.status = 'cancelled'
            then true else false
        end                                         as is_churned,

        -- Subscription duration in days
        case
            when s.ended_at is not null
            then extract(
                day from s.ended_at - s.started_at
            )::int
            else null
        end                                         as duration_days,

        -- CDC metadata
        s.cdc_op,
        s.cdc_ts_ms,
        s.cdc_consumed_at

    from stg_sub s
    left join latest_plans p
        on s.plan_id = p.plan_id

)

select * from final