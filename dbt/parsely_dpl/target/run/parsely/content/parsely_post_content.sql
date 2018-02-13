
       

       delete
  from "blog_dbt_dev"."parsely_post_content"
  where (pageview_post_id) in (
    select (pageview_post_id)
    from "parsely_post_content__dbt_incremental_tmp"
  );

       insert into "blog_dbt_dev"."parsely_post_content" ("metadata", "n", "metadata_save_date_tmsp", "metadata_pub_date_tmsp", "metadata_full_content_word_count", "metadata_duration", "word_count_buckets", "url", "metadata_urls", "metadata_title", "metadata_thumb_url", "metadata_tags", "metadata_share_urls", "metadata_section", "metadata_post_id", "metadata_page_type", "metadata_image_url", "metadata_data_source", "metadata_custom_metadata", "metadata_canonical_url", "metadata_authors", "pageview_post_id", "metadata_save_date_tmsp_tz", "metadata_pub_date_tmsp_tz")
       (
         select "metadata", "n", "metadata_save_date_tmsp", "metadata_pub_date_tmsp", "metadata_full_content_word_count", "metadata_duration", "word_count_buckets", "url", "metadata_urls", "metadata_title", "metadata_thumb_url", "metadata_tags", "metadata_share_urls", "metadata_section", "metadata_post_id", "metadata_page_type", "metadata_image_url", "metadata_data_source", "metadata_custom_metadata", "metadata_canonical_url", "metadata_authors", "pageview_post_id", "metadata_save_date_tmsp_tz", "metadata_pub_date_tmsp_tz"
         from "parsely_post_content__dbt_incremental_tmp"
       );
     