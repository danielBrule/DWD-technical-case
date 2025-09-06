{{ config(materialized='view', schema='intermediate') }}

select distinct
  company_id    as company_id,
  company_name  as company_name
from {{ ref('stg_company') }}
where company_id is not null
  and company_name is not null
