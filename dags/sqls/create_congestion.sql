CREATE TABLE {{ params.output_table_name }} AS
SELECT
    h_n.gu AS gu,
    ((h_n.subtotal * h_b.subtotal * 0.4) + (s_n.subtotal * s_a.subtotal * 0.3) + p_r.ratio * 0.3) / 10000 AS index
FROM
    hospital_num_per_yr AS h_n
JOIN
    hospital_bed_per_yr AS h_b ON h_n.gu = h_b.gu AND h_n.year = h_b.year
JOIN
    sports_num_per_yr AS s_n ON h_b.gu = s_n.gu AND h_b.year = s_n.year
JOIN
    sports_area_per_yr AS s_a ON s_n.gu = s_a.gu AND s_n.year = s_a.year
JOIN
    park_ratio_per_yr AS p_r ON s_a.gu = p_r.gu AND s_a.year = p_r.gu.year
WHERE
    h_n.year = 2022