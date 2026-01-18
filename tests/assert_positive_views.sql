-- This test ensures view counts are non-negative
SELECT *
FROM {{ ref('fct_messages') }}
WHERE view_count < 0
