SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '10000ms';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

CREATE TABLE vl_web_model (
    id INT,
    name STRING,
    description STRING,
    created_at TIMESTAMP,
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
    'table-name' = 'vl_web.model',
    'username' = 'root',
    'password' = 'Admin@900',
    'server-time-zone' = 'Asia/Shanghai',
    'scan.startup.mode' = 'initial'
);

CREATE TABLE vl_web_model_sink (
    id INT,
    name STRING,
    description STRING,
    created_at TIMESTAMP,
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
    'password' = 'Admin@900'
);

INSERT INTO vl_web_model_sink (id, name, description, created_at, url, access_level, created_by, image_url)
    SELECT id, name, description, created_at, url, access_level, created_by, image_url FROM vl_web_model;

