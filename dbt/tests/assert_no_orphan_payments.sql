-- Every payment must belong to a known subscription
-- Fails if fct_payments has payments with no matching subscription in fct_subscriptions
select p.*
from {{ ref('fct_payments') }} p
left join {{ ref('fct_subscriptions') }} s
    on p.subscription_id = s.subscription_id
where s.subscription_id is null
