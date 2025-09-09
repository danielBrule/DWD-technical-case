{{ config(materialized='view', schema='intermediate') }}

with ranked as (
    select
        df.fund_sk,
        df.fund_name,
        sf.transaction_date,
        sf.transaction_type,
        safe_cast(sf.transaction_amount as numeric) as transaction_amount,
        safe_cast(sf.transaction_index as int64)    as transaction_index,
        row_number() over (
            partition by df.fund_sk, sf.transaction_date, sf.transaction_type
            order by safe_cast(sf.transaction_index as int64) desc
        ) as rn
    from {{ ref('stg_fund') }} sf
    join {{ ref('dim_fund') }} df
      on df.fund_name = sf.fund_name
)

select
    fund_sk,
    fund_name,
    transaction_date,
    transaction_type,
    transaction_amount,
    transaction_index
from ranked
where rn = 1