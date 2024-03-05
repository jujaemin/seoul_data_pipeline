CREATE TABLE {{ params.output_table_name }} AS
SELECT
  {{ params.col_to_select }}
FROM
  {{ params.source_database }}.{{ params.source_table }}
{{ params.extra }}