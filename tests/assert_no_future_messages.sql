-- This test ensures no Telegram message has a future date
SELECT *
FROM {{ ref('fct_messages') }}
WHERE date_key > CURRENT_DATE
