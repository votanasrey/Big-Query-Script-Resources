
DECLARE date1, exec_date1 DATE;
SET date1 = DATE_SUB(CURRENT_DATE(), INTERVAL 5 DAY);
SET exec_date1 = CURRENT_DATE();

CREATE OR REPLACE TABLE `foodpanda-kh-bigquery.pandata_kh.country_KH_marketing_input_voucher_error_messages_report` AS ( 

  WITH voucher_actions_raw AS (
    SELECT
      EXTRACT(DATE FROM TIMESTAMP_ADD(event_timestamp_utc, INTERVAL 7 HOUR)) AS date_local,
      CASE
        WHEN event_action = 'order_coupon.submitted' AND cd47 IS NOT NULL THEN LOWER(cd47)
        ELSE LOWER(COALESCE(cd47, cd27, 'NA'))
      END AS input_voucher_code,
      CASE
        WHEN cd48 IS NOT NULL AND (NOT REGEXP_CONTAINS(cd48, r'Api|Exception') OR REGEXP_CONTAINS(cd48, r'Domain')) THEN 'Other Errors'
        WHEN cd48 IS NOT NULL AND REGEXP_CONTAINS(cd48, r'Api|Exception') THEN cd48
        ELSE 'NA'
      END AS error_message,
      COUNT(DISTINCT ga_session_id) AS total_attempts
      FROM `fulfillment-dwh-production.pandata_curated.ga_events`
    WHERE 
      partition_date <= exec_date1 - 2
      AND partition_date >= date1
      AND global_entity_id = 'FP_KH'
      AND event_action IN ('order_coupon.submitted', 'order_coupon.failed')
    GROUP BY 1,2,3
  )

  SELECT
    date_local,
    input_voucher_code,
    error_message,
    total_attempts,
    SUM(total_attempts) OVER(PARTITION BY date_local, input_voucher_code) AS total_attempts_all_errors_daily
  FROM voucher_actions_raw

)


