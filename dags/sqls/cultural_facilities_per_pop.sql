CREATE TABLE {{ params.output_database }}.{{ params.output_table }} AS 
SELECT
 ppm.year_month,
 ppm.gu,
 ROUND(SUM(sc.library+sc.theater+sc.gallery+sc.cinema+sc.museum)/AVG(ppm.total_living_pop) * 10000, 2) as total_cultural_rate,
 ROUND(SUM(sc.library)/AVG(ppm.total_living_pop) * 10000, 2) as library,
 ROUND(SUM(sc.theater)/AVG(ppm.total_living_pop) * 10000, 2) as theater,
 ROUND(SUM(sc.gallery)/AVG(ppm.total_living_pop) * 10000, 2) as gallery,
 ROUND(SUM(sc.cinema)/AVG(ppm.total_living_pop) * 10000, 2) as cinema,
 ROUND(SUM(sc.museum)/AVG(ppm.total_living_pop) * 10000, 2) as museum
FROM
 ad_hoc.pop_per_month AS ppm
JOIN
 raw_data.seoul_culture AS sc
ON
 ppm.gu = sc.gu
GROUP BY
 ppm.year_month,
 ppm.gu
ORDER BY
 ppm.year_month,
 total_cultural_rate DESC