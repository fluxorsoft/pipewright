-- Performance against observed outcomes: the latest score of each key from this model version, with its outcome from
-- monitoring.performance.labels, in the shape the metrics templates read (the label column and the _pw_ outputs).
CREATE OR REPLACE TABLE `{out_table}`
OPTIONS (labels = [{labels}], expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {keep_days} DAY))
AS
SELECT lb.`{outcome}` AS `{label}`, {outputs}
FROM `{output_table}` AS pr
JOIN `{labels_table}` AS lb USING (`{key}`)
WHERE pr.model_version_id = '{model_version}'
QUALIFY ROW_NUMBER() OVER (PARTITION BY pr.`{key}` ORDER BY pr.scored_at DESC) = 1
