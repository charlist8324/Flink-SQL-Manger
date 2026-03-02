
CREATE TABLE source_table (
    id INT,
    name STRING,
    timestamp TIMESTAMP(3)
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1'
);

CREATE TABLE sink_table (
    id INT,
    name STRING,
    timestamp TIMESTAMP(3)
) WITH (
    'connector' = 'print'
);

INSERT INTO sink_table
SELECT id, name, timestamp FROM source_table;
