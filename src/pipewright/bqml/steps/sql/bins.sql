-- Rows per baseline bin (as numpy's searchsorted(edges, v, side="right") counts them in the container path).
SELECT RANGE_BUCKET(v, ARRAY<FLOAT64>[{edges}]) AS bucket, COUNT(*) AS n
FROM (SELECT CAST(`{column}` AS FLOAT64) AS v FROM `{table}` WHERE {where})
WHERE v IS NOT NULL
GROUP BY bucket
