select
    s.message_id,
    c.channel_key,
    d.date_key,
    s.message_text,
    s.message_length,
    s.views,
    s.forwards as forward_count,
    s.has_image
from "medical_warehouse"."staging"."stg_telegram_messages" s
join "medical_warehouse"."staging"."dim_channels" c
  on s.channel_name = c.channel_name
join "medical_warehouse"."staging"."dim_dates" d
  on s.message_date = d.date_key