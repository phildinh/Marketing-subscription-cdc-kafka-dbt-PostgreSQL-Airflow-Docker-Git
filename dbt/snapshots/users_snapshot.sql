-- =============================================================
-- users_snapshot.sql
-- SCD Type 2 snapshot for users
-- Tracks every version of a user over time
-- Triggers on any change to email or plan_id
-- =============================================================

{% snapshot users_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='user_id',
        strategy='timestamp',
        updated_at='cdc_consumed_at',
        invalidate_hard_deletes=True
    )
}}

select
    user_id,
    email,
    name,
    plan_id,
    created_at,
    cdc_op,
    cdc_ts_ms,
    cdc_consumed_at

from {{ ref('stg_users') }}

{% endsnapshot %}