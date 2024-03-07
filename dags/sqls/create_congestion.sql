CREATE TABLE {{ params.output_table_name }} AS
SELECT 
    division_name,
	COUNT(
		CASE
			WHEN avg_speed < 23 THEN 1 ELSE NULL
		END
	) / AVG(avg_speed) AS calculated
FROM '{{ params.source_database }}'.'seoul_road'
GROUP BY division_name