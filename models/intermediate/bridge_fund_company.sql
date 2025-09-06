{{ config(materialized='view', schema='intermediate') }}

select distinct
  df.fund_sk,
  dc.company_id
from {{ ref('stg_company') }} sc
join {{ ref('dim_fund') }}    df on df.fund_name = sc.fund_name
join {{ ref('dim_company') }} dc on dc.company_id = sc.company_id and dc.company_name = sc.company_name
