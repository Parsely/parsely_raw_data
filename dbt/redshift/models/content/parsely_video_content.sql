-- 1 row per content with most recent metdata

{{
    config(
        materialized='incremental',
        sql_where='TRUE',
        unique_key='metadata_canonical_url'
    )
}}

with most_recent_incoming_videos as (
  select
    metadata_canonical_url,
    max(ts_action) as ts_action
  from {{ref('parsely_base_events')}}
  group by metadata_canonical_url
),

dedupe as (
  select
    metadata_canonical_url,
    metadata,
    metadata_authors,
    metadata_custom_metadata,
    metadata_duration,
    metadata_data_source,
    metadata_full_content_word_count,
    metadata_image_url,
    metadata_page_type,
    metadata_post_id,
    metadata_pub_date_tmsp,
    metadata_save_date_tmsp,
    metadata_pub_date_tmsp_tz,
    metadata_save_date_tmsp_tz,
    metadata_section,
    metadata_share_urls,
    metadata_tags,
    metadata_thumb_url,
    metadata_title,
    metadata_urls,
    url,
    row_number() over (partition by metadata_canonical_url order by ts_action) as n
  from {{ref('parsely_base_events')}}
  inner join most_recent_incoming_videos using (metadata_canonical_url, ts_action)
)

select
  *
from dedupe
where n  = 1
