SELECT
  MIN(park_area_ratio) AS min_value,
  MAX(park_area_ratio) AS max_value
FROM
  {{ params.source_database }}.{{ params.source_table }};

CREATE TABLE {{ params.output_table_name }} AS
SELECT
  gu,
  (park_area_ratio-min_value)/(max_value-min_value) AS normalized_value
FROM
  {{ params.source_database }}.{{ params.source_table }}
{{ params.extra }};