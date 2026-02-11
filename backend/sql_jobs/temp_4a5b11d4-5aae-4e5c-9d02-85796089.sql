CREATE TABLE alembic_version (
    version_num STRING
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

CREATE TABLE t2 (
    alembic_version STRING
) WITH (
    'connector' = 'print'
);

INSERT INTO t2
    SELECT version_num FROM alembic_version;

