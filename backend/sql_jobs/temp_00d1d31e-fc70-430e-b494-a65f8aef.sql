CREATE TABLE alembic_version (
    version_num STRING,
    PRIMARY KEY (version_num) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '10.160.10.246',
    'port' = '3306',
    'database-name' = 'vl_web',
    'table-name' = 'vl_web.alembic_version',
    'username' = 'root',
    'password' = 'Admin@900',
    'server-time-zone' = 'Asia/Shanghai'
);

CREATE TABLE alembic_version_sink (
    version_num STRING
) WITH (
    'connector' = 'print'
);

INSERT INTO alembic_version_sink
    SELECT version_num FROM alembic_version;

