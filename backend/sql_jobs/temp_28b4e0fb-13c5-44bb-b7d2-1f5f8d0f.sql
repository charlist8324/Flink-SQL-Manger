SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '30000ms';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.dir' = 'file:///home/flink-1.18.1/checkpoint/checkpoints';

CREATE TABLE `ka_user` (
    `id` INT,
    `name` STRING,
    `sex` STRING,
    PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'ka_user',
    'properties.bootstrap.servers' = '192.168.31.251:9092',
    'properties.group.id' = 'flink-consumer',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'properties.auto.offset.reset' = 'latest',
    'properties.enable.auto.commit' = 'false'
);

CREATE TABLE `ka_address` (
    `id` INT,
    `address` STRING,
    `phone` STRING,
    PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'ka_address',
    'properties.bootstrap.servers' = '192.168.31.251:9092',
    'properties.group.id' = 'flink-consumer',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'properties.auto.offset.reset' = 'latest',
    'properties.enable.auto.commit' = 'false'
);

CREATE TABLE `ka_order` (
    `id` INT,
    `product` STRING,
    `order` STRING,
    PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'ka_order',
    'properties.bootstrap.servers' = '192.168.31.251:9092',
    'properties.group.id' = 'flink-consumer',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'properties.auto.offset.reset' = 'latest',
    'properties.enable.auto.commit' = 'false'
);

CREATE TABLE `ka_user_sink` (
    `id` INT,
    `name` STRING,
    `sex` STRING,
    PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://192.168.31.234:3306/target',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'table-name' = 'ka_user_sink',
    'username' = 'root',
    'password' = 'Admin@900'
);

CREATE TABLE `ka_address_sink` (
    `id` INT,
    `address` STRING,
    `phone` STRING,
    PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://192.168.31.234:3306/target',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'table-name' = 'ka_address_sink',
    'username' = 'root',
    'password' = 'Admin@900'
);

CREATE TABLE `ka_order_sink` (
    `id` INT,
    `product` STRING,
    `order` STRING,
    PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://192.168.31.234:3306/target',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'table-name' = 'ka_order_sink',
    'username' = 'root',
    'password' = 'Admin@900'
);

BEGIN STATEMENT SET;

INSERT INTO `ka_user_sink`
SELECT `id`, `name`, `sex` FROM `ka_user`;

INSERT INTO `ka_address_sink`
SELECT `id`, `address`, `phone` FROM `ka_address`;

INSERT INTO `ka_order_sink`
SELECT `id`, `product`, `order` FROM `ka_order`;

END;
