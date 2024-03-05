CREATE TABLE {{ params.output_table_name }} AS
SELECT
    m_n.gu AS gu,
    ((m_n.subtotal * m_b.subtotal * 0.4) + (s_n.subtotal * s_a.subtotal * 0.3) + p.ratio * 0.3 * 1000) / 10000 AS index
FROM
    medical_num AS m_n
JOIN
    medical_bed AS m_b ON m_n.gu = m_b.gu AND m_n.year = m_b.year
JOIN
    sports_num AS s_n ON m_b.gu = s_n.gu AND m_b.year = s_n.year
JOIN
    sports_area AS s_a ON s_n.gu = s_a.gu AND s_n.year = s_a.year
JOIN
    raw_data.seoul_park AS p ON s_a.gu = p.gu AND s_a.year = p.year
WHERE
    m_n.year = 2022