SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '30000ms';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

CREATE TABLE vl_web_alembic_version (
    version_num STRING,
    PRIMARY KEY (version_num) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '10.160.10.246',
    'port' = '3306',
    'database-name' = 'vl_web',
    'table-name' = 'alembic_version',
    'username' = 'root',
    'password' = 'Admin@900',
    'server-time-zone' = 'Asia/Shanghai',
    'scan.incremental.snapshot.enabled' = 'true',
    'scan.startup.mode' = 'initial',
    'debezium.snapshot.locking.mode' = 'none'
);

CREATE TABLE vl_web_model (
    id INT,
    name STRING,
    description STRING,
    created_at TIMESTAMP(3),
    url STRING,
    access_level STRING,
    created_by INT,
    image_url STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '10.160.10.246',
    'port' = '3306',
    'database-name' = 'vl_web',
    'table-name' = 'model',
    'username' = 'root',
    'password' = 'Admin@900',
    'server-time-zone' = 'Asia/Shanghai',
    'scan.incremental.snapshot.enabled' = 'true',
    'scan.startup.mode' = 'initial',
    'debezium.snapshot.locking.mode' = 'none'
);

CREATE TABLE vl_web_user (
    id INT,
    username STRING,
    email STRING,
    password_hash STRING,
    role STRING,
    created_at TIMESTAMP(3),
    permissions STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '10.160.10.246',
    'port' = '3306',
    'database-name' = 'vl_web',
    'table-name' = 'user',
    'username' = 'root',
    'password' = 'Admin@900',
    'server-time-zone' = 'Asia/Shanghai',
    'scan.incremental.snapshot.enabled' = 'true',
    'scan.startup.mode' = 'initial',
    'debezium.snapshot.locking.mode' = 'none'
);

CREATE TABLE vl_web_model_sink (
    id INT,
    name STRING,
    description STRING,
    created_at TIMESTAMP(3),
    url STRING,
    access_level STRING,
    created_by INT,
    image_url STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://10.178.80.102:3306/test',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'table-name' = 'vl_web_model',
    'username' = 'root',
    'password' = 'Admin@900',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s',
    'sink.max-retries' = '3'
);

CREATE TABLE vl_web_user_sink (
    id INT,
    username STRING,
    email STRING,
    password_hash STRING,
    role STRING,
    created_at TIMESTAMP(3),
    permissions STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://10.178.80.102:3306/test',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'table-name' = 'vl_web_user',
    'username' = 'root',
    'password' = 'Admin@900',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s',
    'sink.max-retries' = '3'
);

CREATE TABLE vl_web_alembic_version_sink (
    version_num STRING,
    PRIMARY KEY (version_num) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://10.178.80.102:3306/test',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'table-name' = 'vl_web_alembic_version',
    'username' = 'root',
    'password' = 'Admin@900',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s',
    'sink.max-retries' = '3'
);

BEGIN STATEMENT SET;

INSERT INTO vl_web_model_sink (id, name, description, created_at, url, access_level, created_by, image_url)
    SELECT id, name, description, created_at, url, access_level, created_by, image_url FROM vl_web_model;

INSERT INTO vl_web_user_sink (id, username, email, password_hash, role, created_at, permissions)
    SELECT id, username, email, password_hash, role, created_at, permissions FROM vl_web_user;

INSERT INTO vl_web_alembic_version_sink (version_num)
    SELECT version_num FROM vl_web_alembic_version;

END;
