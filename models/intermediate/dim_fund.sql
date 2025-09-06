{{ config(materialized='view', schema='intermediate') }}

select
  {{ dbt_utils.generate_surrogate_key(['fund_name']) }} as fund_sk,
  fund_name,
  fund_size
from {{ ref('stg_fund') }}
group by fund_name, fund_size