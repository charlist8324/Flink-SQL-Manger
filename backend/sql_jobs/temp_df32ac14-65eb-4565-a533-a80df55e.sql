CREATE TABLE vl_web_user (
    id INT,
    username STRING,
    email STRING,
    password_hash STRING,
    role STRING,
    created_at TIMESTAMP,
    permissions STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '10.160.10.246',
    'port' = '3306',
    'database-name' = 'vl_web',
    'table-name' = 'vl_web.user',
    'username' = 'root',
    'password' = 'Admin@900',
    'server-time-zone' = 'Asia/Shanghai',
    'scan.incremental.snapshot.enabled' = 'false'
);

CREATE TABLE vl_web_user_sink (
    id INT,
    username STRING,
    email STRING,
    password_hash STRING,
    role STRING,
    created_at TIMESTAMP,
    permissions STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://10.178.80.102:3306/test',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'table-name' = 'vl_web_user',
    'username' = 'root',
    'password' = 'Admin@900'
);

INSERT INTO vl_web_user_sink (id, username, email, password_hash, role, created_at, permissions)
    SELECT id, username, email, password_hash, role, created_at, permissions FROM vl_web_user;

