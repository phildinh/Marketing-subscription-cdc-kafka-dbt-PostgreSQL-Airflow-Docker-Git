-- =============================================================
-- fct_payments.sql
-- Fact table — one row per payment transaction
-- Grain: one row per payment_id
-- Key metrics: revenue, failure rate, retry rate
-- =============================================================

with stg_pay as (

    -- Deduplicate to one row per payment_id — latest CDC event wins
    select distinct on (payment_id)
        *
    from {{ ref('stg_payments') }}
    order by payment_id, cdc_ts_ms desc

),

stg_sub as (

    select distinct on (subscription_id)
        subscription_id,
        user_id,
        plan_id
    from {{ ref('stg_subscriptions') }}
    order by subscription_id, cdc_ts_ms desc

),

final as (

    select
        -- Keys
        p.payment_id,
        p.subscription_id,
        s.user_id,
        s.plan_id,

        -- Payment details
        p.amount,
        p.status,
        p.created_at,

        -- Flags
        p.is_retry,

        case
            when p.status = 'success'
            then true else false
        end                                         as is_successful,

        case
            when p.status = 'failed'
            then true else false
        end                                         as is_failed,

        case
            when p.status = 'refunded'
            then true else false
        end                                         as is_refunded,

        -- Revenue — only count successful non-refunded payments
        case
            when p.status = 'success'
            then p.amount else 0
        end                                         as recognised_revenue,

        -- CDC metadata
        p.cdc_op,
        p.cdc_ts_ms,
        p.cdc_consumed_at

    from stg_pay p
    left join stg_sub s
        on p.subscription_id = s.subscription_id

)

select * from final