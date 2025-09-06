{{ config(materialized='view', schema='intermediate') }}

select
  df.fund_sk,
  sf.transaction_date,
  lower(sf.transaction_type)              as transaction_type,
  safe_cast(sf.transaction_amount as numeric) as amount,
  safe_cast(sf.transaction_index as int64)    as transaction_index
from {{ ref('stg_fund') }} sf
join {{ ref('dim_fund') }} df
  on df.fund_name = sf.fund_name
