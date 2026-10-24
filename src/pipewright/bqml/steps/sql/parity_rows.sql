-- Promotion by files: the source's test rows the destination must score the same way, as JSON: every row with a
-- missing value among the model's inputs (an exported model fills them differently from BigQuery ML) and up to
-- {sample} others, in a fixed order, with the score ML.PREDICT gave them.
SELECT TO_JSON_STRING(STRUCT({columns})) AS instance, {expected} AS expected, {any_null} AS has_null
FROM `{test_table}`
WHERE {expected} IS NOT NULL
QUALIFY {any_null} OR ROW_NUMBER() OVER (ORDER BY FARM_FINGERPRINT(TO_JSON_STRING(STRUCT({columns})))) <= {sample}
