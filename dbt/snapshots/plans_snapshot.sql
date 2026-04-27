-- =============================================================
-- plans_snapshot.sql
-- SCD Type 2 snapshot for plans
-- Tracks every version of a plan over time
-- Critical for price history — price increases trigger new version
-- =============================================================

{% snapshot plans_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='plan_id',
        strategy='timestamp',
        updated_at='cdc_consumed_at',
        invalidate_hard_deletes=True
    )
}}

select
    plan_id,
    name,
    price,
    billing_cycle,
    created_at,
    cdc_op,
    cdc_ts_ms,
    cdc_consumed_at

from {{ ref('stg_plans') }}

{% endsnapshot %}