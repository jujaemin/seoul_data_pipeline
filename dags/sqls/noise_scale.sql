CREATE TABLE {{ params.output_database }}.{{ params.output_table }} AS 
SELECT
    '대한민국' AS country,
	'서울특별시' AS city,
    DATE_FORMAT(registered_date, '%Y-%m') as "Year_Month", gu as Gu, ROUND(AVG(avg_noise)) as "Avg_Noise"
FROM
    "raw_data"."seoul_noise"
WHERE
    gu NOT IN ('서울대공원')
GROUP BY
    DATE_FORMAT(registered_date, '%Y-%m'), gu
ORDER BY
    DATE_FORMAT(registered_date, '%Y-%m'), gu