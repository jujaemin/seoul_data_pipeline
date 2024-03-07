CREATE TABLE {{ params.output_database }}.{{ params.output_table }} AS (
    WITH price_per_sqm AS (
    SELECT
        DATE_FORMAT(contracted_date, '%Y-%m') AS year_month,
        gu,
        CAST(ROUND(price / house_area) AS INTEGER) AS price_per_sqm
    FROM
        "raw_data"."seoul_housing"
    )
    SELECT
        year_month,
        gu,
        MIN(price_per_sqm) as min_price_per_sqm,
        MAX(price_per_sqm) as max_price_per_sqm,
        CAST(ROUND(AVG(price_per_sqm)) AS INTEGER) as avg_price_per_sqm
    FROM
        price_per_sqm
    GROUP BY
        1,2
    ORDER BY
        1,5
)