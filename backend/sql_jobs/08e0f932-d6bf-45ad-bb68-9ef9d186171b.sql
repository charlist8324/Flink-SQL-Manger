CREATE TABLE test_source (
    id STRING,
    name STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'datagen',
    'format' = 'json'
);

CREATE TABLE test_target (
    id STRING,
    name STRING
) WITH (
    'connector' = 'print',
    'format' = 'json'
);

