CREATE TABLE s (
    id STRING,
    name STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'datagen',
    'format' = 'json'
);

CREATE TABLE t (
    id STRING,
    name STRING
) WITH (
    'connector' = 'print',
    'format' = 'json'
);

