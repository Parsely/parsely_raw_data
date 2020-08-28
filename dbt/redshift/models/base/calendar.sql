-- Generates a standard calendar table
SELECT
    (DATE_PART('y', date_gen.dt)*10000+DATE_PART('mon', date_gen.dt)*100+DATE_PART('day', date_gen.dt))::int AS "id",
    date_gen.dt AS "date",
    DATE_PART('y', date_gen.dt)::smallint AS "year",
    DATE_PART('mon', date_gen.dt)::smallint AS "month",
    DATE_PART('day', date_gen.dt)::smallint AS "day",
    DATE_PART('qtr', date_gen.dt)::smallint AS "quarter",
    DATE_PART('w', date_gen.dt)::smallint AS "week",
    CASE DATE_PART('dow', date_gen.dt)
      WHEN 0 THEN 'Sunday'
      WHEN 1 THEN 'Monday'
      WHEN 2 THEN 'Tuesday'
      WHEN 3 THEN 'Wednesday'
      WHEN 4 THEN 'Thursday'
      WHEN 5 THEN 'Friday'
      WHEN 6 THEN 'Saturday'
      END::VARCHAR(9) AS "day_name",
    CASE DATE_PART('mon', date_gen.dt)::smallint
      WHEN 1 THEN 'January'
      WHEN 2 THEN 'February'
      WHEN 3 THEN 'March'
      WHEN 4 THEN 'April'
      WHEN 5 THEN 'May'
      WHEN 6 THEN 'June'
      WHEN 7 THEN 'July'
      WHEN 8 THEN 'August'
      WHEN 9 THEN 'September'
      WHEN 10 THEN 'October'
      WHEN 11 THEN 'November'
      WHEN 12 THEN 'December'
      END::VARCHAR(9) AS "month_name",
    CASE
      WHEN DATE_PART('dow', date_gen.dt)::smallint IN (0,6) THEN TRUE
      ELSE FALSE
      END::boolean AS "weekend_flag"
FROM
(SELECT
  ('2050-01-01' - n)::date AS dt FROM (SELECT row_number() over () AS n FROM stl_scan LIMIT 54787)) date_gen
