CREATE TABLE t1 (
    id STRING,
    name STRING
) WITH (
    'connector' = 'datagen',
    'format' = 'json'
);

CREATE TABLE t2 (
    id STRING,
    name STRING
) WITH (
    'connector' = 'print',
    'format' = 'json'
);

INSERT INTO t2
    SELECT id, name FROM t1;

