SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '30000ms';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.dir' = 'file:///home/flink-1.18.1/checkpoint/checkpoints';

CREATE TABLE `ka_address` (
    `id` INT,
    `address` STRING,
    `phone` STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'ka_address',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'flink-consumer',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'properties.auto.offset.reset' = 'latest',
    'properties.enable.auto.commit' = 'false'
);

CREATE TABLE `ka_address_sink` (
    `id` INT,
    `address` STRING,
    `phone` STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://192.168.31.234:3306/target',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'table-name' = 'ka_address_sink',
    'username' = 'root',
    'password' = 'Admin@900'
);

INSERT INTO `ka_address_sink`
SELECT `id`, `address`, `phone` FROM `ka_address`;

