-- Preparation in SQL: one SELECT over {rows}, the run's rows (the entity table with its features). Pipewright writes
-- its result into the run's rows before training, runs it again on the rows it scores, and pins it with the release:
-- the model records the digest of the file it trained with. Keep the key, the cut-off and the label columns.
-- An endpoint runs no SQL: a caller of an online endpoint sends charges_per_month itself.
SELECT
  *,
  SAFE_DIVIDE(total_charges, NULLIF(tenure_months, 0)) AS charges_per_month
FROM {rows}
