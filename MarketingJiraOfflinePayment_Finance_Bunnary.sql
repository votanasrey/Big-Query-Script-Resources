
DECLARE period DATE;
SET period = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH), MONTH);

WITH jira_offline_payment_table AS (
    SELECT 

        payment.created_date AS created_at, 
        payment.updated_date AS updated_at,
        DATE(payment.created_date) AS created_date,
        DATE(payment.updated_date) AS updated_date,
        DATE(payment.start_date) AS start_date,
        DATE(payment.end_date) AS end_date,
        SUBSTRING(STRING(DATE(payment.created_date)),1,7) AS month_date,
        payment.ticket,
        payment.cost_type,
        payment.costs_overall,
        payment.version_date,
        (CASE
            WHEN DATE_DIFF(DATE(payment.start_date), DATE(payment.end_date), DAY) <= 30 THEN '1 MONTH'
            WHEN DATE_DIFF(DATE(payment.start_date), DATE(payment.end_date), DAY) <= 60 THEN '2 MONTH'
            WHEN DATE_DIFF(DATE(payment.start_date), DATE(payment.end_date), DAY) <= 90 THEN '3 MONTH'
        END) AS period,
        /*(CASE 
            WHEN DATE(payment.created_date) + 30 THEN "1 MONTH"
            WHEN DATE(payment.created_date) + 60 THEN "2 MONTH"
            WHEN DATE(payment.created_date) + 90 THEN "3 MONTH"
        END),*/
        /*(CASE 
            WHEN DATE(payment.version_date) < (DATE(payment.created_date) + 30) THEN "1 Month" 
            WHEN DATE(payment.version_date) < (DATE(payment.created_date) + 60) THEN "2 Month" 
            WHEN DATE(payment.version_date) < (DATE(payment.created_date) + 90) THEN "3 Month" 
        END) AS period_date,*/
        --GENERATE_DATE_ARRAY(DATE(payment.created_date), CURRENT_DATE(), INTERVAL 1 MONTH) AS period_payment 

        ROW_NUMBER() OVER (PARTITION BY payment.ticket, payment.cost_type, CAST(payment.costs_overall AS STRING) ORDER BY payment.version_date DESC) AS rank_updated_date,
    FROM `fulfillment-dwh-production.pandata_report.marketing_cost_report_offline_jira` AS payment
    WHERE 
        payment.country_iso IN ("KH")
        AND DATE(payment.created_date) >= period 
        AND DATE(payment.start_date) >= period
        --AND DATE_DIFF(DATE(payment.start_date), DATE(payment.end_date), MONTH) >= 30
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
), result_table AS(
    SELECT 
        * 
        --EXCEPT(rank_updated_date) 
    FROM jira_offline_payment_table
    WHERE rank_updated_date = 1
)

SELECT * FROM result_table
--WHERE ticket = 'OMFPKH-1409'
ORDER BY ticket, rank_updated_date

