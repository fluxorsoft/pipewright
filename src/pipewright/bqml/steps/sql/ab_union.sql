-- FR-57: a scoring run during an A/B test: each arm's predictions together, with the arm beside each row.
CREATE OR REPLACE TABLE `{out_table}`
OPTIONS (labels = [{labels}], expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {keep_days} DAY))
AS
SELECT *, 'control' AS _pw_arm FROM `{control}`
UNION ALL
SELECT *, 'treatment' AS _pw_arm FROM `{treatment}`
