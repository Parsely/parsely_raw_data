
       

       delete
  from "blog_dbt_dev"."parsely_event_ids"
  where (event_id) in (
    select (event_id)
    from "parsely_event_ids__dbt_incremental_tmp"
  );

       insert into "blog_dbt_dev"."parsely_event_ids" ("event_id")
       (
         select "event_id"
         from "parsely_event_ids__dbt_incremental_tmp"
       );
     