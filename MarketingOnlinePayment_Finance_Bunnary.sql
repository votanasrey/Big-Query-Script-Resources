

DECLARE period DATE;
SET period = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH), MONTH);

WITH marketing_online_payment_table AS(
    SELECT
        online.global_entity_id,
        online.country_iso,
        online.country,
        online.partner,
        online.channel,
        online.subchannel,
        online.campaign,
        online.vertical,
        online.platform,
        online.cost_eur,
        online.cost_eur_rr,
        online.date AS campaign_date,
    FROM `fulfillment-dwh-production.pandata_report.marketing_cost_report_online` AS online 
    WHERE
        online.global_entity_id = 'FP_KH'
        AND online.country_iso = 'KH'
        AND online.country = 'Cambodia'
), result_table_v1 AS(
    SELECT 
        campaign,
        country,
        partner,
        channel,
        subchannel,
        vertical,
        platform,
        SUBSTRING(STRING(DATE(campaign_date)),1,7) AS month_date_range,
        ROUND(SUM(cost_eur),2) AS total_cost_eur
    FROM marketing_online_payment_table
    GROUP BY 1,2,3,4,5,6,7,8
)
SELECT * FROM result_table_v1
--WHERE campaign = 'sem_bra_ios_eat_ma_KH_EN'
--WHERE campaign IN (SELECT campaign FROM result_table_v1 GROUP BY 1 HAVING COUNT(1)>1)
ORDER BY campaign, month_date_range

