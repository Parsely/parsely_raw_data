-- 1 row per content with most recent metdata

{{
    config(
        materialized='incremental',
        sql_where='TRUE',
        unique_key='pageview_post_id'
    )
}}

with most_recent_incoming_posts as (
  select
    pageview_post_id,
    max(ts_action) as ts_action
  from {{ref('parsely_base_events')}}
  group by pageview_post_id
),

dedupe as (
  select
    pageview_post_id,
    metadata	,
    metadata_authors	,
    metadata_canonical_url	,
    metadata_custom_metadata	,
    metadata_duration	,
    metadata_data_source	,
    metadata_full_content_word_count	,
    metadata_image_url	,
    metadata_page_type	,
    metadata_post_id	,
    metadata_pub_date_tmsp	,
    metadata_save_date_tmsp	,
    metadata_section	,
    metadata_share_urls	,
    metadata_tags	,
    metadata_thumb_url	,
    metadata_title	,
    metadata_urls	,
    url,
    row_number() over (partition by pageview_post_id order by ts_action) as n
  from {{ref('parsely_base_events')}}
  inner join most_recent_incoming_posts using (pageview_post_id, ts_action)
)

select
  *
from dedupe
where n=1
