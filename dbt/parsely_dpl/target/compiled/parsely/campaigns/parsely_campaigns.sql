-- 1 row per campaign
-- metrics: pageviews, engaged time, videostarts, video engaged time, visitors



with incoming_campaigns as (
  select
    utm_id,
    utm_campaign,
    utm_medium,
    utm_source,
    utm_term,
    utm_content,
    engaged_time,
    pageviews,
    video_engaged_time,
    videoviews,
--  dedupe field
    row_number() over (partition by utm_id order by ts_session_current) as n
  from "blog_dbt_dev"."parsely_pageviews_sessionized"
),



relevant_existing as (

    select
        *
    from "blog_dbt_dev"."parsely_campaigns"
    where utm_id in (select utm_id from incoming_campaigns)

),

-- left join fields from old data: min_tstamp
unioned as (

    select
      *
    from incoming_campaigns

    union all

    select
        *
    from relevant_existing

),

merged as (

    select
      n,
      utm_id,
      utm_campaign,
      utm_medium,
      utm_source,
      utm_term,
      utm_content,
      sum(engaged_time) as engaged_time,
      sum(pageviews) as pageviews,
      sum(video_engaged_time) as video_engaged_time,
      sum(videoviews) as videoviews
    from unioned
    group by n, utm_id, utm_campaign, utm_medium, utm_source, utm_term, utm_content

),



dedupe as (
    select
      *
    from merged
)

select
  *
from dedupe
where n = 1