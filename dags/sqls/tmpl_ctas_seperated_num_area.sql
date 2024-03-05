CREATE TABLE {{ params.output_table_name }} AS
SELECT
  'year',
	gu,
	subtotal
FROM
  {{ params.source_database }}.{{ params.source_table }}
WHERE
  category = '{{ params.col_category }}' AND
  gu != '소계' 
  {{ params.extra }}