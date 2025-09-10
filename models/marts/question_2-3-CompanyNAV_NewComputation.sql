{{ config(materialized='table', schema='marts') }}

-- 1) Company valuation totals 
with company_totals as (
  select
    fund_name,
    valuation_date,
    sum(valuation_amount) as total_company_valuation
  from {{ ref('fct_company_valuation') }}
  group by fund_name, valuation_date
),

-- 2) Keep only dates where BOTH a NAV and company valuations exist, compute scale factor
scales as (
  select
    fn.fund_name,
    fn.date,
    fn.nav,
    ct.total_company_valuation,
    case
      when ct.total_company_valuation is not null and ct.total_company_valuation <> 0
        then fn.nav / ct.total_company_valuation
      else null
    end as scale_pct
  from {{ ref('question_2-1_FundNAV') }} fn
  join company_totals ct
    on ct.fund_name = fn.fund_name
   and ct.valuation_date = fn.date
  where ct.total_company_valuation is not null
    and ct.total_company_valuation <> 0
),

-- 3) Apply the scale to each company valuation on that date
scaled as (
  select
    c.fund_name,
    c.company_id,
    c.company_name,
    c.valuation_date             as nav_date,
    c.valuation_amount           as company_valuation,
    s.nav as fund_nav,
    s.total_company_valuation,
    s.scale_pct,
    safe_cast(c.valuation_amount * s.scale_pct as numeric) as company_nav
  from  {{ ref('fct_company_valuation') }} c
  join scales s
    on s.fund_name = c.fund_name
   and s.date = c.valuation_date
)

-- Final output
select
  fund_name,
  company_id,
  company_name,
  nav_date,
  company_valuation,
  fund_nav,
  total_company_valuation,
  scale_pct,
  company_nav
from scaled
order by fund_name, nav_date, company_id
