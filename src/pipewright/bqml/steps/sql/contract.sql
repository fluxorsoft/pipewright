-- The data contract's checks, counted in one pass: Pipewright compares each count with its rule.
SELECT COUNT(*) AS _rows{checks}
FROM `{rows_table}`
