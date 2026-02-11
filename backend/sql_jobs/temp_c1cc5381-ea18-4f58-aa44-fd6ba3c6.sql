CREATE TABLE t1 (
    dd STRING
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1'
);

CREATE TABLE t2 (

) WITH (
    'connector' = 'print'
);

INSERT INTO t2
    SELECT dd FROM t1;

