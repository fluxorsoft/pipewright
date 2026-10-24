{score} AS _pw_score,
IF({score} >= {threshold}, '{positive}', '{negative}') AS _pw_label
