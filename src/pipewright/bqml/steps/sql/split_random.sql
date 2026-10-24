MOD(ABS(FARM_FINGERPRINT(CONCAT('{seed}', TO_JSON_STRING(STRUCT({keys}))))), 1000000) < {test_millionths}
