-- Promotion by files: a model exported in the source environment, imported here from its copied files.
CREATE OR REPLACE MODEL `{model}` OPTIONS (MODEL_TYPE = 'TENSORFLOW', MODEL_PATH = '{path}')
