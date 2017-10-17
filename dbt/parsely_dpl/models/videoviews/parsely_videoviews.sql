-- 1 row per videoview
-- sum engaged time for all heartbeats
-- metrics: videoviews, engaged time

with events as (

    select * from parsely_transform_videoviews

),

-- xf = transformed in fishtown slang
events_xf as (

    select *

    from events

)

select
