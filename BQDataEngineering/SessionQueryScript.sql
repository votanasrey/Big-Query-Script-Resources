--CREATE OR REPLACE TABLE `foodpanda-kh-bigquery.pandata_kh.rpt_daily_sessions` AS

DELETE FROM `foodpanda-kh-bigquery.pandata_kh.rpt_daily_sessions`
WHERE order_month >= current_date()-5;

INSERT INTO `foodpanda-kh-bigquery.pandata_kh.rpt_daily_sessions`
SELECT 
date(date_utc) as order_month,
global_entity_id, 
count(distinct ga_user_id) as customers,
count(distinct ga_session_id)as sessions,
sum(totals.visits) as visits,
sum(totals.time_on_screen) as time_on_screen,
sum(totals.time_on_site) as time_on_site,

FROM `fulfillment-dwh-production.pandata_curated.ga_sessions` 
WHERE global_entity_id in ("FP_KH") 
and partition_date >= current_date()-5
AND date(date_utc) >= current_date()-5
group by 1,2