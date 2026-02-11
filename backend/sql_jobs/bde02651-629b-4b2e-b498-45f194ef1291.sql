CREATE TABLE t1 (
    id STRING,
    name STRING,
    PRIMARY KEY (id) NOT ENFORCED
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

