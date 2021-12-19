
CREATE TABLE IF NOT EXISTS db1.kafka_metric_src
(
    id String
    , dt DateTime
    , metric String
    , val Nullable(Float32)
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'rc1a-ipedhaa0dlfrq8cm.mdb.yandexcloud.net:9091'
    , kafka_topic_list = 'ch_metric'
    , kafka_group_name = 'ch_cg'
    , kafka_format = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS db1.kafka_metric_mt
(
    id String
    , dt DateTime
    , metric String
    , val Nullable(Float32)
) ENGINE = MergeTree()
order by uid;

CREATE MATERIALIZED VIEW db1.kafka_metric_mv to db1.kafka_metric_mt
    as select * from db1.kafka_metric_src

select metric, avg(val) avg_value from db1.kafka_metric_mt group by metric
order by metric
;