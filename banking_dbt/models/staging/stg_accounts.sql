{{ config(materialized='view') }}

with ranked as (
    select
        (data->>'id')::varchar            as account_id,
        (data->>'customer_id')::varchar   as customer_id,
        (data->>'account_type')::varchar  as account_type,
        (data->>'balance')::float        as balance,
        (data->>'currency')::varchar      as currency,
        (data->>'created_at')::timestamp as created_at,
        current_timestamp       as load_timestamp,
        row_number() over (
            partition by (data->>'id')::varchar
            order by (data->>'created_at')::timestamp desc
        ) as rn
    from {{ source('raw', 'raw_accounts') }}
)

select
    account_id,
    customer_id,
    account_type,
    balance,
    currency,
    created_at,
    load_timestamp
from ranked
where rn = 1