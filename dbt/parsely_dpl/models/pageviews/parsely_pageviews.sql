-- 1 row per pageview
-- sum engaged time for all heartbeats
-- metrics: pageviews, engaged time

with events as (

    select * from parsely_transform_pageviews

),

-- xf = transformed in fishtown slang
events_xf as (

    select *,
        (TIMESTAMP 'epoch' + left(metadata_pub_date_tmsp,10)::bigint * INTERVAL '1 Second ') as publish_time,
        (TIMESTAMP 'epoch' + left(timestamp_info_nginx_ms,10)::bigint * INTERVAL '1 Second ') as read_time

    from events

)

select


    datediff(hour, publish_time, read_time) as hours_since_publish,
    datediff(day, publish_time, read_time) as days_since_publish,
    datediff(week, publish_time, read_time) as weeks_since_publish,
