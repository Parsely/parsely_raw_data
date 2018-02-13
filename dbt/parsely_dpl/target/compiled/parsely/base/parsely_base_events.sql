
  select
      *
  from "blog_dbt_dev"."parsely_all_events"
  where action in ('pageview','heartbeat','videostart','vheartbeat')
--  and event_id not in
--    (select event_id from "blog_dbt_dev"."parsely_event_ids")
  and ua_browser <> 'Googlebot'
  --add in logic for custom:excludebottraffic== 'Yes'
