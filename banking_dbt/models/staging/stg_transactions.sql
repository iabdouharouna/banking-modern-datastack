{{ config(materialized='view') }}

SELECT
    (data->>'id')::varchar                 AS transaction_id,
    (data->>'account_id')::varchar         AS account_id,
    (data->>'amount')::float              AS amount,
    (data->>'txn_type')::varchar           AS transaction_type,
    (data->>'related_account_id')::varchar AS related_account_id,
    (data->>'status')::varchar             AS status,
    (data->>'created_at')::timestamp      AS transaction_time,
    CURRENT_TIMESTAMP            AS load_timestamp
FROM {{ source('raw', 'raw_transactions') }}