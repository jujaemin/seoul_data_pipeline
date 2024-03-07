CREATE TABLE {{ params.output_database }}.{{ params.output_table }} AS 
SELECT
 	'대한민국' AS country,
	'서울특별시' AS city,
	d.division AS division,
	d.gu AS gu,
	c.calculated AS calculated
FROM
  "ad_hoc"."dim_div_by_gu" AS d
LEFT JOIN
  "analytics"."congestion_by_division" AS c
  ON d.division = c.division_name