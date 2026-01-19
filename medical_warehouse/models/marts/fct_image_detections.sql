with detections as (
    select * from raw.image_detections
),
messages as (
    select * from {{ ref('fct_messages') }}
)

select
    d.message_id,
    m.channel_key,
    m.date_key,
    d.detected_class,
    d.confidence as confidence_score,
    d.image_category,
    d.image_path
from detections d
join messages m
  on d.message_id = m.message_id
