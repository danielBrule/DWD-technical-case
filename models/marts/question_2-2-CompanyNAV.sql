{{ config(materialized='table', schema='marts') }}


-- 1) reporting dates: use all dates from company and valuation
with reporting_dates as (
  select distinct fund_name, valuation_date as asof_date
  from {{ ref('fct_company_valuation') }}
  union distinct
  select distinct fund_name, transaction_date as asof_date
  from {{ ref('fct_fund_event') }}
),

-- 2) For each reporting date, get the latest company valuation available as of that date
company_val_asof as (
  select
    d.fund_name,
    c.company_name,
    d.asof_date,
    -- latest valuation on or before as-of date
    (array_agg(c.valuation_amount order by c.valuation_date desc limit 1))[offset(0)] as company_valuation
  from reporting_dates d
  join {{ ref('fct_company_valuation') }} c
    on c.fund_name = d.fund_name
   and c.valuation_date  <= d.asof_date
  group by d.fund_name, c.company_name, d.asof_date
),

-- 3) filtered view of fund events that keeps only commitment transactions)
commitments as (
  select
    fund_name,
    transaction_date,
    transaction_amount
  from {{ ref('fct_fund_event') }}
  where transaction_type = 'Commitment'
),

-- 4) compute cumulative commitments per reporting date 
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

-- 5) Given everything committed up to this date, what fraction of the fund (based on its size) does that represent
--    ownership % = commitments / fund_size (latest size from dim_fund)
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

-- 6) final result: scale company valuation by ownership
select
  v.fund_name,
  v.company_name,
  v.asof_date               as nav_date,
  v.company_valuation,
  o.ownership_pct,
  safe_cast(v.company_valuation * o.ownership_pct as numeric) as company_nav
from company_val_asof v
left join ownership_pct o
  on o.fund_name = v.fund_name
 and o.asof_date = v.asof_date
order by fund_name, nav_date, company_name