{{ config(
    materialized='table',
    schema='staging'
) }}

with src as (
  select
    trim(`fund_name`)                               as fund_name,
    safe_cast(`_fund_size_` as int64)                 as fund_size,
    nullif(initcap(trim(`transaction_type`)), '')   as transaction_type,
    safe_cast(ceil(`transaction_index`) as int64)         as transaction_index,
    safe_cast(`transaction_date` as date)           as transaction_date,
    safe_cast(`_transaction_amount_` as numeric)      as transaction_amount,
    nullif(initcap(trim(`sector`)), '')             as sector,
    nullif(trim(`country`), '')                     as country_raw,
    nullif(initcap(trim(`region`)), '')             as region,
    _FILE_NAME as source_filename,
    current_timestamp()                             as ingested_at
  from {{ source('excel_raw','fund_data_ext') }}
),
map as (
  select
    {{ normalize_country_key('raw_value') }} as key_norm,
    country_standard
  from {{ ref('country_map') }}
),
joined as (
  select
    s.*,
    m.country_standard as country
  from src s
  left join map m
    on {{ normalize_country_key('s.country_raw') }} = m.key_norm
)
select 
    fund_name,
    fund_size,
    transaction_type,
    transaction_index,
    transaction_date,
    transaction_amount,
    sector,
    country,
    region,
    source_filename,
    ingested_at
from joined
