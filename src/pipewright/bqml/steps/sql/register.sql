-- Model Registry, after gate 1: the model stays in BigQuery, the registry gets a new version of the use case's model.
ALTER MODEL `{model}` SET OPTIONS (vertex_ai_model_id = '{registry_id}')
