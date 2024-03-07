SELECT
  MIN(subtotal) AS min_value,
  MAX(subtotal) AS max_value
FROM
  {{ params.source_database }}.{{ params.source_table }};

CREATE TABLE {{ params.output_table_name }} AS
SELECT
  'year',
	gu,
	(subtotal-min_value)/(max_value-min_value) AS normalized_value
FROM
  {{ params.source_database }}.{{ params.source_table }}
WHERE
  category = '{{ params.col_category }}' AND
  gu != '소계' 
  {{ params.extra }};