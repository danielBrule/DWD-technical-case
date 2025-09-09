{{ config(materialized='table', schema='marts') }}

-- Source references:
--   fct_company_valuation: (fund_name, company_id, company_name, val_date, valuation_amount, transaction_index)
--   fct_fund_event:       (fund_name, transaction_date, transaction_type, amount, transaction_index)
--   dim_fund:             (fund_name, fund_size)

-- 1) reporting dates: use valuation dates seen at company level
with reporting_dates as (
  select distinct fund_name, valuation_date as asof_date
  from {{ ref('fct_company_valuation') }}
  union distinct
  select distinct fund_name, transaction_date as asof_date
  from {{ ref('fct_fund_event') }}

),

-- 2) latest company valuation as-of each date
company_val_asof as (
  select
    d.fund_name,
    c.company_id,
    c.company_name,
    d.asof_date,
    -- latest valuation on or before as-of date
    (array_agg(c.valuation_amount order by c.valuation_date desc limit 1))[offset(0)] as company_valuation
  from reporting_dates d
  join {{ ref('fct_company_valuation') }} c
    on c.fund_name = d.fund_name
   and c.valuation_date  <= d.asof_date
  group by d.fund_name, c.company_id, c.company_name, d.asof_date
),

-- 3) cumulative commitments up to each as-of date (ownership numerator)
commitments as (
  select
    fund_name,
    transaction_date,
    transaction_amount
  from {{ ref('fct_fund_event') }}
  where transaction_type = 'Commitment'
),
ownership_base as (
  select
    d.fund_name,
    d.asof_date,
    coalesce(sum(c.transaction_amount), 0) as total_commitments_to_date
  from reporting_dates d
  left join commitments c
    on c.fund_name = d.fund_name
   and c.transaction_date <= d.asof_date
  group by d.fund_name, d.asof_date
),

-- 4) ownership % = commitments / fund_size (latest size from dim_fund)
ownership_pct as (
  select
    o.fund_name,
    o.asof_date,
    case
      when df.fund_size is not null and df.fund_size > 0
        then o.total_commitments_to_date / df.fund_size
      else 0
    end as ownership_pct
  from ownership_base o
  left join {{ ref('dim_fund') }} df
    on df.fund_name = o.fund_name
)

-- 5) final result: scale company valuation by ownership
select
  v.fund_name,
  v.company_id,
  v.company_name,
  v.asof_date               as nav_date,
  v.company_valuation,
  o.ownership_pct,
  safe_cast(v.company_valuation * o.ownership_pct as numeric) as company_nav
from company_val_asof v
left join ownership_pct o
  on o.fund_name = v.fund_name
 and o.asof_date = v.asof_date
order by fund_name, nav_date, company_id
