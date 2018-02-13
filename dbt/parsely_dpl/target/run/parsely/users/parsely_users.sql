
       

       delete
  from "blog_dbt_dev"."parsely_users"
  where (apikey_visitor_id) in (
    select (apikey_visitor_id)
    from "parsely_users__dbt_incremental_tmp"
  );

       insert into "blog_dbt_dev"."parsely_users" ("days_since_last_session", "user_total_video_engaged_time", "user_total_videoviews", "user_total_engaged_time", "user_total_pageviews", "user_counter", "user_engagement_level", "user_type", "visitor_site_id", "apikey_visitor_id", "apikey", "date_last_seen", "date_first_seen", "last_timestamp")
       (
         select "days_since_last_session", "user_total_video_engaged_time", "user_total_videoviews", "user_total_engaged_time", "user_total_pageviews", "user_counter", "user_engagement_level", "user_type", "visitor_site_id", "apikey_visitor_id", "apikey", "date_last_seen", "date_first_seen", "last_timestamp"
         from "parsely_users__dbt_incremental_tmp"
       );
     