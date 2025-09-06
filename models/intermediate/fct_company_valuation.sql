{{ config(materialized='view', schema='intermediate') }}

select
  df.fund_sk,
  df.fund_name,
  dc.company_id,
  dc.company_name,
  sc.transaction_date                         as valuation_date,
  safe_cast(sc.transaction_amount as numeric) as valuation_amount,
  safe_cast(sc.transaction_index as int64)    as transaction_index
from {{ ref('stg_company') }} sc
join {{ ref('dim_fund') }}    df on df.fund_name = sc.fund_name
join {{ ref('dim_company') }} dc on dc.company_id = sc.company_id and dc.company_name = sc.company_name
where sc.transaction_type = 'Valuation'
