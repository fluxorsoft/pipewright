-- bqml.import: a model trained elsewhere, imported instead of trained (its files were read by the runner).
CREATE OR REPLACE MODEL `{model}` OPTIONS (MODEL_TYPE = '{format}', MODEL_PATH = '{path}')
