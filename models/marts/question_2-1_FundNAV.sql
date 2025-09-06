{{ config(materialized='view', schema='marts') }}




-- 1) One valuation per fund/date (latest by transaction_index)
with valuations as (
  select
    fund_sk,
    transaction_date,
    transaction_amount as valuation_amount,
    row_number() over (
      partition by fund_sk, transaction_date
      order by transaction_index desc
    ) as rn
  from {{ ref('fct_fund_event') }}
  where lower(transaction_type) = 'valuation'
),
clean_valuations as (
  select fund_sk, transaction_date, valuation_amount
  from valuations
  where rn = 1
),

-- 2) One non-valuation flow per fund/date (latest by transaction_index)
tmp_call as (
  select
    fund_sk,
    transaction_date,
    transaction_amount as flow_amount,
    row_number() over (
      partition by fund_sk, transaction_date
      order by transaction_index desc
    ) as rn
  from {{ ref('fct_fund_event') }}
  where lower(transaction_type) in ('call')
),
clean_call as (
  select fund_sk, transaction_date, flow_amount
  from tmp_call
  where rn = 1
),

-- 3) All event dates to report on
event_dates as (
  select distinct fund_sk, transaction_date as event_date
  from {{ ref('fct_fund_event') }}
),

-- 4) Anchor valuation date = latest valuation on/before each as-of date
anchor as (
  select
    e.fund_sk,
    e.event_date,
    max(v.transaction_date) as anchor_date
  from event_dates e
  left join clean_valuations v
    on v.fund_sk = e.fund_sk
   and v.transaction_date <= e.event_date
  group by e.fund_sk, e.event_date
),

-- 5) Anchor valuation amount (pick amount at that anchor_date)
anchor_with_amount as (
  select
    a.fund_sk,
    a.event_date,
    a.anchor_date,
    -- pick valuation_amount corresponding to the chosen anchor_date
    any_value(cv.valuation_amount) as anchor_val
  from anchor a
  left join clean_valuations cv
    on cv.fund_sk = a.fund_sk
   and cv.transaction_date = a.anchor_date
  group by a.fund_sk, a.event_date, a.anchor_date
),

-- 6) Sum flows strictly after anchor_date up to as-of date
flow_since_anchor as (
  select
    awa.fund_sk,
    awa.event_date,
    coalesce(sum(cf.flow_amount), 0) as flow_since_anchor
  from anchor_with_amount awa
  left join clean_call cf
    on cf.fund_sk = awa.fund_sk
   and cf.transaction_date >  awa.anchor_date
   and cf.transaction_date <= awa.event_date
  group by awa.fund_sk, awa.event_date
)

-- 7) NAV = anchor valuation + flows since anchor
select
  fund_name,
  awa.event_date as date,
  awa.anchor_val + fsa.flow_since_anchor as nav
from anchor_with_amount awa
join flow_since_anchor fsa
  using (fund_sk, event_date)
join {{ ref('dim_fund') }}
  using (fund_sk, fund_sk)
where awa.anchor_val is not null
order by fund_sk, date