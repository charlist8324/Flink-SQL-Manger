SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '30000ms';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

CREATE TABLE db_source_user (
    id INT,
    name STRING,
    sex STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '192.168.31.251',
    'port' = '3306',
    'database-name' = 'db_source',
    'table-name' = 'user',
    'username' = 'root',
    'password' = 'Admin@900',
    'server-time-zone' = 'Asia/Shanghai',
    'scan.incremental.snapshot.enabled' = 'true',
    'scan.startup.mode' = 'initial',
    'debezium.snapshot.locking.mode' = 'none'
);

CREATE TABLE db_source_user_sink (
    id INT,
    name STRING,
    sex STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'doris',
    'fenodes' = '192.168.31.234:8030',
    'table.identifier' = 'testdb.db_source_user_sink',
    'username' = 'root',
    'password' = 'Admin@900',
    'sink.properties.format' = 'json',
    'sink.properties.strip_outer_array' = 'false'
);

INSERT INTO db_source_user_sink (id, name, sex)
    SELECT id, name, sex FROM db_source_user;

