
       

       delete
  from "blog_dbt_dev"."parsely_campaigns"
  where (utm_id) in (
    select (utm_id)
    from "parsely_campaigns__dbt_incremental_tmp"
  );

       insert into "blog_dbt_dev"."parsely_campaigns" ("n", "videoviews", "video_engaged_time", "pageviews", "engaged_time", "utm_content", "utm_term", "utm_source", "utm_medium", "utm_campaign", "utm_id")
       (
         select "n", "videoviews", "video_engaged_time", "pageviews", "engaged_time", "utm_content", "utm_term", "utm_source", "utm_medium", "utm_campaign", "utm_id"
         from "parsely_campaigns__dbt_incremental_tmp"
       );
     