WITH source AS (
    SELECT *
    FROM raw.telegram_messages
),
cleaned AS (
    SELECT
        message_id::BIGINT,
        channel_name::TEXT,
        message_date::TIMESTAMP,
        message_text::TEXT,
        views::INTEGER,
        forwards::INTEGER,
        has_media::BOOLEAN,
        image_path::TEXT,
        LENGTH(message_text) AS message_length,
        CASE WHEN has_media THEN TRUE ELSE FALSE END AS has_image
    FROM source
    WHERE message_text IS NOT NULL
)
SELECT *
FROM cleaned
