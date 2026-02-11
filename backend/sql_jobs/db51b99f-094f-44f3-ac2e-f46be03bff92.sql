CREATE TABLE t1 (
    id STRING,
    name STRING
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1'
);

CREATE TABLE t2 (
    id STRING,
    name STRING
) WITH (
    'connector' = 'print'
);

INSERT INTO t2
    SELECT id, name FROM t1;

