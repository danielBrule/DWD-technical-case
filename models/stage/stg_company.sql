{{ config(
    materialized='table',
    schema='staging'
) }}

with src as (
  select
    trim(`fund_name`)                              as fund_name,
    safe_cast(`company_id` as int64)               as company_id,
    trim(`company_name`)                           as company_name,
    nullif(initcap(trim(`transaction_type`)), '')  as transaction_type,
    safe_cast(`transaction_index` as int64)        as transaction_index,
    safe_cast(`transaction_date` as date)          as transaction_date,
    safe_cast(`_transaction_amount_` as numeric)     as transaction_amount,
    case when lower(trim(`sector`)) = 'n/a' then null else nullif(initcap(trim(`sector`)), '') end as sector,
    case when lower(trim(`country`)) = 'n/a' then null else trim(`country`) end as country_raw,
    case when lower(trim(`region`)) = 'n/a' then null else nullif(initcap(trim(`region`)), '') end as region,
    _FILE_NAME                                     as source_filename,  -- keep trace of file
    current_timestamp()                            as ingested_at
  from {{ source('excel_raw','company_data_ext') }}
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
    m.country_standard as country,
  from src s
  left join map m
    on {{ normalize_country_key('s.country_raw') }} = m.key_norm
)

select 
    fund_name,
    company_id,
    company_name,
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
