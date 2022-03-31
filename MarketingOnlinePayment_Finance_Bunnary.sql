

DECLARE period DATE;
SET period = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH), MONTH);

WITH marketing_online_payment_table AS(
    SELECT
        *
    FROM `fulfillment-dwh-production.pandata_report.marketing_cost_report_online` AS online 
    WHERE
        online.global_entity_id = 'FP_KH'
        AND online.country = 'Cambodia'
)
SELECT * FROM marketing_online_payment_table
