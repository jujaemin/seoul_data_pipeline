CREATE TABLE {{ params.output_database }}.{{ params.output_table }} AS 
SELECT  
    "pop"."gu", 
    "pop"."avg_total_living_pop", 
    "park"."gu_area",
    "pop"."avg_total_living_pop"/"park"."gu_area" AS "avg_total_living_pop_per_unit_area"
FROM(   
    SELECT  
        "gu", 
        AVG("total_living_pop") AS "avg_total_living_pop"
    FROM
        "raw_data"."seoul_pop"
    WHERE 
        YEAR("standard_date") = 2024 AND 
        MONTH("standard_date") = 2
    GROUP BY "gu"
) AS pop
JOIN (  
    SELECT  
        "gu", 
        "gu_area"/1000000 AS "gu_area"
    FROM
        "raw_data"."seoul_park"
) AS "park"
ON 
    "pop"."gu" = "park"."gu"