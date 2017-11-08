-- 1 row per campaign
-- metrics: pageviews, engaged time, videostarts, video engaged time, visitors

{{
    config(
        materialized='incremental',
        sql_where='TRUE',
        unique_key='utm_id'
    )
}}

with incoming_campaigns as (
  select
    utm_id,
    utm_campaign,
    utm_medium,
    utm_source,
    utm_term,
    utm_content,
    engaged_time,
    pageview_counter,
    video_engaged_time,
    videoviews
  from {{ref('parsely_pageviews')}}
),

{%if adapter.already_exists(this.schema,this.name)%}

relevant_existing as (

    select
        *
    from {{ this }}
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
      utm_id,
      utm_campaign,
      utm_medium,
      utm_source,
      utm_term,
      utm_content,
      sum(engaged_time) as engaged_time,
      sum(pageview_counter) as pageview_counter,
      sum(video_engaged_time) as video_engaged_time,
      sum(videoviews) as videoviews
    from unioned
    group by utm_id, utm_campaign, utm_medium, utm_source, utm_term, utm_content

)

{% else %}

-- initial run, don't merge
merged as (

    select
      *
    from incoming_campaigns
)

{% endif %}

select
    *
from merged
