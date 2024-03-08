CREATE TABLE {{ params.output_database }}.{{ params.output_table }} AS 
SELECT
    '대한민국' AS country,
    '서울특별시' AS city,
    m_n.gu AS gu,
    ((m_n.normalized_value + m_b.normalized_value) * 0.2 + (s_n.normalized_value + s_a.normalized_value) * 0.15 + p.normalized_value * 0.3) AS index
FROM
    ad_hoc.medical_num AS m_n
JOIN
    ad_hoc.medical_bed AS m_b ON m_n.gu = m_b.gu AND m_n.year = m_b.year
JOIN
    ad_hoc.sports_num AS s_n ON m_b.gu = s_n.gu AND m_b.year = s_n.year
JOIN
    ad_hoc.sports_area AS s_a ON s_n.gu = s_a.gu AND s_n.year = s_a.year
JOIN
    ad_hoc.park_ratio AS p ON s_a.gu = p.gu
WHERE
    m_n.year = 2022