WITH source AS (
    SELECT * FROM raw.telegram_messages
),

cleaned AS (
    SELECT
        message_id,
        channel_name,
        CAST(message_date AS DATE) AS message_date,
        message_text,
        LENGTH(message_text) AS message_length,
        COALESCE(views, 0) AS view_count,
        COALESCE(forwards, 0) AS forward_count,
        COALESCE(has_media, FALSE) AS has_image,
        image_path
    FROM source
    WHERE message_text IS NOT NULL
)

SELECT * FROM cleaned;