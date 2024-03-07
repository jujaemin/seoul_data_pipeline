CREATE TABLE {{ params.output_database }}.{{ params.output_table }} AS 
SELECT
    '대한민국' AS country,
	'서울특별시' AS city,
    "year", gu, subtotal
FROM
	"raw_data"."seoul_green_area"
WHERE
	"year" = 2022 AND
	category = '면적 (㎡)' AND
	gu NOT IN ('소계','중부공원녹지사업소', '안전총괄실')
ORDER BY
	subtotal DESC