SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '30000ms';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

CREATE TABLE db_source_address (
    id INT,
    address STRING,
    phone STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '192.168.31.251',
    'port' = '3306',
    'database-name' = 'db_source',
    'table-name' = 'address',
    'username' = 'root',
    'password' = 'Admin@900',
    'server-time-zone' = 'Asia/Shanghai',
    'scan.incremental.snapshot.enabled' = 'true',
    'scan.startup.mode' = 'initial'
);

CREATE TABLE db_source_order (
    id INT,
    product STRING,
    order STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '192.168.31.251',
    'port' = '3306',
    'database-name' = 'db_source',
    'table-name' = 'order',
    'username' = 'root',
    'password' = 'Admin@900',
    'server-time-zone' = 'Asia/Shanghai',
    'scan.incremental.snapshot.enabled' = 'true',
    'scan.startup.mode' = 'initial'
);

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
    'scan.startup.mode' = 'initial'
);

CREATE TABLE db_source_address_sink (
    id INT,
    address STRING,
    phone STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'doris',
    'fenodes' = '192.168.31.234:8030',
    'table.identifier' = 'testdb.db_source_address_sink',
    'username' = 'root',
    'password' = 'Admin@900',
    'sink.properties.format' = 'json',
    'sink.properties.strip_outer_array' = 'false',
    'sink.label-prefix' = 'label_',
    'sink.check-interval' = '10000'
);

CREATE TABLE db_source_order_sink (
    id INT,
    product STRING,
    order STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'doris',
    'fenodes' = '192.168.31.234:8030',
    'table.identifier' = 'testdb.db_source_order_sink',
    'username' = 'root',
    'password' = 'Admin@900',
    'sink.properties.format' = 'json',
    'sink.properties.strip_outer_array' = 'false',
    'sink.label-prefix' = 'label_',
    'sink.check-interval' = '10000'
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
    'sink.properties.strip_outer_array' = 'false',
    'sink.label-prefix' = 'label_',
    'sink.check-interval' = '10000'
);

BEGIN STATEMENT SET;

INSERT INTO db_source_address_sink
SELECT id, address, phone FROM db_source_address;

INSERT INTO db_source_order_sink
SELECT id, product, order FROM db_source_order;

INSERT INTO db_source_user_sink
SELECT id, name, sex FROM db_source_user;

END;
