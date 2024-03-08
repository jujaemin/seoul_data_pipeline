CREATE TABLE {{ params.output_database }}.{{ params.output_table }} AS(
WITH price_sqm_stats AS (
SELECT
    DATE_FORMAT(contracted_date, '%Y-%m') AS year_month,
    gu,
    price / house_area AS price_per_sqm
FROM
    "raw_data"."seoul_housing"
)
SELECT
  year_month,
  '대한민국' AS country,
  '서울특별시' AS city,
  gu,
  COUNT(*) AS trading_volume,
  AVG(price_per_sqm) * 3.306 as avg_price_per_pyu
FROM
  price_sqm_stats
GROUP BY
  1,4
)
