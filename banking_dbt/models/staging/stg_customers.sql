{{ config(materialized='view') }}

with ranked as (
    select
        (data->>'id')::varchar            as customer_id,
        (data->>'first_name')::varchar    as first_name,
        (data->>'last_name')::varchar     as last_name,
        (data->>'email')::varchar         as email,
        (data->>'created_at')::timestamp as created_at,
        current_timestamp       as load_timestamp,
        row_number() over (
            partition by (data->>'id')::varchar
            order by (data->>'created_at')::timestamp desc
        ) as rn
    from {{ source('raw', 'raw_customers') }}
)

select
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    load_timestamp
from ranked
where rn = 1