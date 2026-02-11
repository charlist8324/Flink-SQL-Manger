CREATE TABLE  (
    version_num VARCHAR,
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
    'format' = 'json'
);

CREATE TABLE alembic_version (
    version_num STRING
) WITH (
    'connector' = 'print'
);

INSERT INTO alembic_version
    SELECT * FROM ;

