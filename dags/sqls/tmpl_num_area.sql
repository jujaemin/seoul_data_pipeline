CREATE TABLE {{ params.output_database }}.{{ params.output_table }} AS
SELECT
  "year",
	gu,
	CAST(((CAST(subtotal AS double)-mm.min_value)/(mm.max_value-mm.min_value)) AS double) AS normalized_value
FROM
  {{ params.source_database }}.{{ params.source_table }}
CROSS JOIN
  (
    SELECT
      MIN(subtotal) AS min_value,
      MAX(subtotal) AS max_value
    FROM
      {{ params.source_database }}.{{ params.source_table }}
    WHERE
      category = '{{ params.col_category }}' AND
      gu != '소계'
  )
  AS mm
WHERE
    category = '{{ params.col_category }}' AND
    gu != '소계'