{{ config(materialized='table', schema='marts') }}



-- 1) All valuations
with clean_valuations as (
  select
    fund_name,
    transaction_date,
    transaction_amount as valuation_amount,
  from {{ ref('fct_fund_event') }}
  where transaction_type = 'Valuation'
),

-- 2) All calls / distribution
clean_call as (
  select
    fund_name,
    transaction_date,
    transaction_amount as flow_amount,
  from {{ ref('fct_fund_event') }}
  where transaction_type in ('Call', 'Distribution')
),

-- 3) All dates to report on
event_dates as (
  select distinct fund_name, transaction_date as event_date
  from {{ ref('fct_fund_event') }}
),

-- 4) Latest valuation date on/before each as-of date
anchor as (
  select
    e.fund_name,
    e.event_date,
    max(v.transaction_date) as anchor_date
  from event_dates e
  left join clean_valuations v
    on v.fund_name = e.fund_name
   and v.transaction_date <= e.event_date
  group by e.fund_name, e.event_date
),

-- 5) valuation amount that belongs to that anchor date
anchor_with_valuation as (
  select
    a.fund_name,
    a.event_date,
    a.anchor_date,
    -- pick valuation_amount corresponding to the chosen anchor_date
    any_value(cv.valuation_amount) as anchor_val
  from anchor a
  left join clean_valuations cv
    on cv.fund_name = a.fund_name
   and cv.transaction_date = a.anchor_date
  group by a.fund_name, a.event_date, a.anchor_date
),

-- 6) add up all the call flows that happened after the anchor valuation date and up to and including the event date
call_since_anchor as (
  select
    awv.fund_name,
    awv.event_date,
    coalesce(sum(cf.flow_amount), 0) as call_since_anchor
  from anchor_with_valuation awv
  left join clean_call cf
    on cf.fund_name = awv.fund_name
   and cf.transaction_date >  awv.anchor_date
   and cf.transaction_date <= awv.event_date
  group by awv.fund_name, awv.event_date
)

-- 7) NAV = anchor valuation + flows since anchor
select
  fund_name,
  awv.event_date as date,
  awv.anchor_val + csa.call_since_anchor as nav
from anchor_with_valuation awv
join call_since_anchor csa
  using (fund_name, event_date)
where awv.anchor_val is not null
order by fund_name, date