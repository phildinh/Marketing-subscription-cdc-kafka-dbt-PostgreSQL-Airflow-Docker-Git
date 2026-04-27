-- MRR must always be positive
-- Fails if any subscription has zero or negative MRR
select *
from {{ ref('fct_subscriptions') }}
where mrr is not null
  and mrr <= 0
