CREATE TABLE park_ratio AS
SELECT
	gu,
	CAST(((CAST(park_area_ratio AS double)-mm.min_value)/(mm.max_value-mm.min_value)) AS double) AS normalized_value
FROM
  raw_data.seoul_park
CROSS JOIN
  (
    SELECT
      MIN(park_area_ratio) AS min_value,
      MAX(park_area_ratio) AS max_value
    FROM
      raw_data.seoul_park
  )
  AS mm