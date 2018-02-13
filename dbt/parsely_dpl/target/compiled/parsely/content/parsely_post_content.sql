-- 1 row per content with most recent metdata



with most_recent_incoming_posts as (
  select
    pageview_post_id,
    max(ts_action) as ts_action
  from "blog_dbt_dev"."parsely_base_events"
  group by pageview_post_id
),

dedupe as (
  select
    pageview_post_id,
    metadata,
    metadata_authors,
    metadata_canonical_url,
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
    case
     when metadata_full_content_word_count >= 4000 then '4,000 or Above'
     when metadata_full_content_word_count >= 3000 then '3,000 - 3,999'
     when metadata_full_content_word_count >= 2000 then '2,000 - 2,999'
     when metadata_full_content_word_count >= 1000 then '1,000 - 1,999'
     when metadata_full_content_word_count >= 500 then '500 - 999'
     when metadata_full_content_word_count >= 100 then '100 - 499'
     else '< 100' end as word_count_buckets,
    row_number() over (partition by pageview_post_id order by ts_action) as n
  from "blog_dbt_dev"."parsely_base_events"
  inner join most_recent_incoming_posts using (pageview_post_id, ts_action)
)

select
  *
from dedupe
where n=1