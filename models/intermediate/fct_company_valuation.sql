{{ config(materialized='view', schema='intermediate') }}

with ranked as (
    select
      df.fund_sk,
      df.fund_name,
      dc.company_id,
      dc.company_name,
      sc.transaction_date                         as valuation_date,
      safe_cast(sc.transaction_amount as numeric) as valuation_amount,
      row_number() over (
        partition by df.fund_sk, dc.company_id, sc.transaction_type, sc.transaction_date
        order by sc.transaction_index desc
      ) as rn
    from {{ ref('stg_company') }} sc
    join {{ ref('dim_fund') }}    df on df.fund_name = sc.fund_name
    join {{ ref('dim_company') }} dc on dc.company_id = sc.company_id and dc.company_name = sc.company_name
    where sc.transaction_type = 'Valuation'
)
select 
    fund_sk,
    fund_name,
    company_id,
    company_name,
    valuation_date,
    valuation_amount
from ranked
where rn = 1
