-- 1 row per visitor_site_Id
-- includes visitor type, returning, new, subscribers, etc
-- first login, last login, etc



-- second time: builds temp table; deletes duplicates by unique key, inserts new data
--


with  __dbt__CTE__parsely_incoming_users as (
-- 1 row per visitor_site_Id
-- includes visitor type, returning, new, subscribers, etc
-- first login, last login, etc



with incoming_users_pageviews as (
  select
      apikey,
      visitor_site_id,
      --custom fields
      apikey_visitor_id,
      -- metrics
      max(ts_session_current_tz) as last_timestamp,
      sum(pageviews) as user_total_pageviews,
      sum(engaged_time) as user_total_engaged_time,
      0 as user_total_videoviews,
      0 as user_total_video_engaged_time
  from "blog_dbt_dev"."parsely_pageviews_sessionized"
  group by 1,2,3
),

incoming_users_videostarts as (
  select
      apikey,
      visitor_site_id,
      --custom fields
      apikey_visitor_id,
      -- metrics
      max(ts_session_current_tz) as last_timestamp,
      0 as user_total_pageviews,
      0 as user_total_engaged_time,
      sum(videoviews) as user_total_videoviews,
      sum(video_engaged_time) as user_total_video_engaged_time
  from "blog_dbt_dev"."parsely_videoviews_sessionized"
  group by 1,2,3
)

select * from incoming_users_pageviews
union all
select * from incoming_users_videostarts
),incoming_users as (
  select
      apikey,
      apikey_visitor_id,
      visitor_site_id,
      -- dates and times
      max(last_timestamp) as last_timestamp,
      date(min(last_timestamp)) as date_first_seen,
      date(max(last_timestamp)) as date_last_seen,
      -- metrics to aggregate
      sum(user_total_pageviews) as user_total_pageviews,
      sum(user_total_engaged_time) as user_total_engaged_time,
      sum(user_total_videoviews) as user_total_videoviews,
      sum(user_total_video_engaged_time) as user_total_video_engaged_time
  from __dbt__CTE__parsely_incoming_users
  group by 1,2,3
),



relevant_existing as (

    select
        apikey,
        apikey_visitor_id,
        visitor_site_id,
        -- dates and times
        eu.last_timestamp,
        eu.date_first_seen,
        eu.date_last_seen,
        -- metrics to aggregate
        eu.user_total_pageviews,
        eu.user_total_engaged_time,
        eu.user_total_videoviews,
        eu.user_total_video_engaged_time
    from "blog_dbt_dev"."parsely_users" as eu
    left join incoming_users as iu using
      (apikey_visitor_id, apikey, visitor_site_id)

),

unioned as (

    -- combined pageviews and videostarts
    select
        apikey,
        apikey_visitor_id,
        visitor_site_id,
        -- dates and times
        last_timestamp,
        date_first_seen,
        date_last_seen,
        -- metrics to aggregate
        user_total_pageviews,
        user_total_engaged_time,
        user_total_videoviews,
        user_total_video_engaged_time
    from incoming_users

    union all

    select
        apikey,
        apikey_visitor_id,
        visitor_site_id,
        -- dates and times
        last_timestamp,
        date_first_seen,
        date_last_seen,
        -- metrics to aggregate
        user_total_pageviews,
        user_total_engaged_time,
        user_total_videoviews,
        user_total_video_engaged_time
    from relevant_existing

),

merged as (

    select
        apikey,
        apikey_visitor_id,
        visitor_site_id,
        -- dates and times
        max(last_timestamp) as last_timestamp,
        min(date_first_seen) as date_first_seen,
        max(date_last_seen) as date_last_seen,
        -- metrics
        sum(user_total_pageviews) as user_total_pageviews,
        sum(user_total_engaged_time) as user_total_engaged_time,
        sum(user_total_videoviews) as user_total_videoviews,
        sum(user_total_video_engaged_time) as user_total_video_engaged_time
    from unioned
    group by 1,2,3


)



select
    1 as user_counter,
    apikey,
    apikey_visitor_id,
    visitor_site_id,
    -- dates and times
    last_timestamp,
    date_first_seen,
    date_last_seen,
    -- metrics to aggregate
    user_total_pageviews,
    user_total_engaged_time,
    user_total_videoviews,
    user_total_video_engaged_time,
    -- derived fields
    case when date_first_seen < date(SYSDATE)
      then 'Returning'
      else 'New' end as user_type,
    case when user_total_pageviews>=30
      then 'Loyalty'
      else 'Non-Loyalty' end as user_engagement_level,
    DATEDIFF(day, last_timestamp, SYSDATE) as days_since_last_session
  from merged