--bigquery-public-data.ml_datasets.credit_card_default 
-- 
create table `endless-radar-330119.advde_ds_us.credit_card_default` as
SELECT
		*
	FROM
		`bigquery-public-data.ml_datasets.credit_card_default`;

select count(*), stat
  from (
select id
       , case when (t.tables.score < 0.5 and t.tables.value = '1' or t.tables.score >= 0.5 and t.tables.value = '0') then 'OK' else 'ERROR' end stat
  from `advde_ds_us.credit_card_default` ,
unnest(predicted_default_payment_next_month) as t
)
group by stat
;

select id
       , case when t.tables.score >= 0.5 and t.tables.value = '0' then 'OK' else 'ERROR' end 
  from `advde_ds_us.credit_card_default` ,
unnest(predicted_default_payment_next_month) as t
where case when t.tables.score >= 0.5 and t.tables.value = '0' then 'OK' else 'ERROR' end = 'ERROR';

-- select id
--        , case when t.tables.score >= 0.5 and t.tables.value = '0' then 'OK' else 'ERROR' end state
--        , t.tables.score, t.tables.value, t.tables
--   from `advde_ds_us.credit_card_default` ,
-- unnest(predicted_default_payment_next_month) as t
-- where case when t.tables.score >= 0.5 and t.tables.value = '0' then 'OK' else 'ERROR' end = 'ERROR';


