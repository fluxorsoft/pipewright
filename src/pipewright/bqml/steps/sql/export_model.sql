-- Promotion by files: the approved model's files (TensorFlow SavedModel; with TRANSFORM a transform/ model beside
-- it) into the run's records folder, read by `pipewright promote`. A DDL statement: it takes no job key.
EXPORT MODEL `{model}` OPTIONS (URI = '{uri}')
