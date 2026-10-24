-- Promotion by files: the parity rows scored by the imported model here, in the order they were given.
WITH given AS (
  SELECT i AS _pw_i, {casts}
  FROM UNNEST([{rows}]) AS j WITH OFFSET AS i
)
SELECT p._pw_raw._pw_i AS i, {outputs}
FROM ML.PREDICT(MODEL `{model}`, ({inner})) AS p
ORDER BY i
