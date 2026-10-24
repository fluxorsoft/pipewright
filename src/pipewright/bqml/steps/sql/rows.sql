-- The run's rows: the source (the entity table, with its features at each row's cut-off and the label window when
-- declared), the team's prepare.sql when it has one, and the held-out test flag. Written once per run, so training,
-- evaluation and the record read fixed rows, never a live view.
CREATE TABLE `{rows_table}`
OPTIONS (labels = [{labels}], description = 'Pipewright BigQuery ML rows of run {run_id}')
AS
WITH prepared AS (
{prepared}
)
SELECT prepared.*, {test_flag} AS _pw_test
FROM prepared
