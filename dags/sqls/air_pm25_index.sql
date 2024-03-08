CREATE TABLE {{ params.output_database }}.{{ params.output_table }} AS 
SELECT
    DATE_FORMAT(measured_date, '%Y-%m') as "Year_Month", gu,
    ROUND(AVG(pm25)) AS avg_pm25,
    CASE
      WHEN AVG(pm25) < 16 THEN '좋음'
      WHEN AVG(pm25) > 15 AND AVG(pm25) < 26 THEN '보통'
      WHEN AVG(pm25) > 25 AND AVG(pm25) < 38 THEN '나쁨'
      WHEN AVG(pm25) > 37 AND AVG(pm25) < 76 THEN '매우나쁨'
      WHEN AVG(pm25) > 75 THEN '최악'
    END AS air_pm25_index
FROM "raw_data"."seoul_air"
GROUP BY
    DATE_FORMAT(measured_date, '%Y-%m'), gu
ORDER BY
    "Year_Month", avg_pm25