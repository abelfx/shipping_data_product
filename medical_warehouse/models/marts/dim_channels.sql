with source as (
    select *
    from {{ ref('stg_telegram_messages') }}
)

select
    row_number() over () as channel_key,
    channel_name,
    case
        when channel_name ilike '%pharma%' then 'Pharmaceutical'
        when channel_name ilike '%cosmetic%' then 'Cosmetics'
        else 'Medical'
    end as channel_type,
    min(message_date) as first_post_date,
    max(message_date) as last_post_date,
    count(*) as total_posts,
    avg(views) as avg_views
from source
group by channel_name
