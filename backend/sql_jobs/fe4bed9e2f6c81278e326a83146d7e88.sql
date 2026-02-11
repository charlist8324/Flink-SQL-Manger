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
    'server-time-zone' = 'Asia/Shanghai',
    'scan.incremental.snapshot.enabled' = 'false'
);

CREATE TABLE version_num_sink (
    version_num STRING
) WITH (
    'connector' = 'print'
);

INSERT INTO version_num_sink
    SELECT version_num FROM alembic_version;

