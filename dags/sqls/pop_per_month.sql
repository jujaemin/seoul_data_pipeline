CREATE TABLE {{ params.output_database }}.{{ params.output_table }} AS 
SELECT
    DATE_FORMAT(seoul_pop.standard_date, '%Y-%m') AS year_month,
    seoul_pop.gu,
    seoul_pop.total_living_pop
FROM
    raw_data.seoul_pop
WHERE
    seoul_pop.gu != '서울시'