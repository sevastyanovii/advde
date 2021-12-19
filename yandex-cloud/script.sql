CREATE DATABASE db1 COMMENT 'Comment1';

CREATE TABLE IF NOT EXISTS db1.kafka_src
(
    uid String
    , dt DateTime
    , val Nullable(Float32)
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'rc1a-ipedhaa0dlfrq8cm.mdb.yandexcloud.net:9091'
    , kafka_topic_list = 'ch_data'
    , kafka_group_name = 'ch_cg'
    , kafka_format = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS db1.kafka_mt
(
    uid String
    , dt DateTime
    , val Nullable(Float32)
) ENGINE = MergeTree()
order by uid;

CREATE MATERIALIZED VIEW db1.kafka_mv to db1.kafka_mt
    as select * from db1.kafka_src;

select count(*), sum(val) from db1.kafka_mt;

select h, count(*), sum(val) from db1.kafka_mt group by extract(hour from dt) as h;