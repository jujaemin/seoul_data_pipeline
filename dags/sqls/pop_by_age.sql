CREATE TABLE {{ params.output_database }}.{{ params.output_table }} AS 
SELECT 
    "gu",
	"sex",
	"age_group",
	"year",
	"pop_by_age",
	CAST("pop_by_age" AS DOUBLE) / SUM("pop_by_age") OVER(PARTITION BY "gu", "sex", "year") AS "age_group_ratio"
FROM (
    SELECT 
        "gu",
        "sex",
        "age_group",
        "year",
        AVG("pop_by_age") AS "pop_by_age"
    FROM (
        SELECT 
            "gu",
            "sex",
            "age",
            CASE
                WHEN age LIKE '0~4세'
                OR age LIKE '5~9세' THEN '10대 미만'
                WHEN age LIKE '10~14세'
                OR age LIKE '15~19세' THEN '10대'
                WHEN age LIKE '20~24세'
                OR age LIKE '25~29세' THEN '20대'
                WHEN age LIKE '30~34세'
                OR age LIKE '35~39세' THEN '30대'
                WHEN age LIKE '40~44세'
                OR age LIKE '45~49세' THEN '40대'
                WHEN age LIKE '50~54세'
                OR age LIKE '55~59세' THEN '50대'
                WHEN age LIKE '60~64세'
                OR age LIKE '65~69세' THEN '60대'
                WHEN age LIKE '70~74세'
                OR age LIKE '75~79세' THEN '70대'
                WHEN age LIKE '80~84세'
                OR age LIKE '85~89세' THEN '80대'
                WHEN age LIKE '90~94세'
                OR age LIKE '95세 이상+' THEN '90대 이상'
            END AS "age_group",
            "year",
            "pop_by_age"
        FROM 
            "raw_data"."seoul_pop_by_age"
        WHERE 
            "gu" != '합계'
            AND "sex" != '계'
            AND "age" != '계'
    )
    GROUP BY 
        "gu",
        "sex",
        "age_group",
        "year"
)