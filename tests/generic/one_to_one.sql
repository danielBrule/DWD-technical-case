{% test one_to_one(model, column_a, column_b) %}

-- Fail if any A maps to > 1 B, or any B maps to > 1 A
with a_many as (
  select
    {{ column_a }} as key_value,
    count(distinct {{ column_b }}) as distinct_other
  from {{ model }}
  where {{ column_a }} is not null and {{ column_b }} is not null
  group by {{ column_a }}
  having count(distinct {{ column_b }}) > 1
),
b_many as (
  select
    {{ column_b }} as key_value,
    count(distinct {{ column_a }}) as distinct_other
  from {{ model }}
  where {{ column_a }} is not null and {{ column_b }} is not null
  group by {{ column_b }}
  having count(distinct {{ column_a }}) > 1
)
select 'A maps to many B' as issue, cast(key_value as string) as key_value, distinct_other
from a_many
union all
select 'B maps to many A' as issue, cast(key_value as string) as key_value, distinct_other
from b_many

{% endtest %}
