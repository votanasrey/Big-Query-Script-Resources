
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
        GENERATE_DATE_ARRAY(DATE(payment.start_date), DATE(payment.end_date), INTERVAL 1 DAY) AS date_range,
        ROW_NUMBER() OVER (PARTITION BY payment.ticket, payment.cost_type ORDER BY payment.version_date DESC) AS rank_updated_date,
    FROM `fulfillment-dwh-production.pandata_report.marketing_cost_report_offline_jira` AS payment
    WHERE 
        payment.country_iso IN ("KH")
        --AND DATE(payment.created_date) >= period 
        --AND DATE(payment.start_date) >= period
), result_table_v1 AS(
    SELECT 
        * 
        --EXCEPT(rank_updated_date) 
    FROM jira_offline_payment_table
    WHERE rank_updated_date = 1
), result_table_v2 AS(
    SELECT * EXCEPT(date_range) 
    FROM result_table_v1,
    UNNEST(date_range) AS date_day_range
), result_table_v3 AS(
    SELECT 
        ticket,
        start_date,
        end_date,
        cost_type,
        costs_overall,
        date_day_range,
        SUBSTRING(STRING(DATE(date_day_range)),1,7) AS month_date_range,
    FROM result_table_v2
    GROUP BY 1,2,3,4,5,6
), result_table_v4 AS(
    SELECT 
        ticket,
        start_date,
        end_date,
        cost_type,
        costs_overall,
        COUNT(DISTINCT date_day_range) AS date_day_range
    FROM result_table_v3
    GROUP BY 1,2,3,4,5
),result_table_v5 AS(
    SELECT 
        ticket,
        start_date,
        end_date,
        cost_type,
        (costs_overall/date_day_range) AS avg_costs_overall_per_day
    FROM result_table_v4
),result_table_v6 AS(
    SELECT 
        ticket,
        start_date,
        end_date,
        cost_type,
        costs_overall,
        month_date_range,
        COUNT(date_day_range) OVER(PARTITION BY month_date_range ORDER BY date_day_range) AS date_day_range
        --SUM(costs_overall)/COUNT(ROW_NUMBER() OVER(PARTITION BY month_date_range ORDER BY ticket))
    FROM result_table_v3
), result_table_v7 AS(
    SELECT  
        ticket,
        start_date,
        end_date,
        cost_type,
        month_date_range,
        costs_overall,
        COUNT(date_day_range) AS count_date_day_range
    FROM result_table_v6
    GROUP BY 1,2,3,4,5,6
),result_table_last_version AS(
    SELECT DISTINCT
        v5.ticket,
        v5.start_date,
        v5.end_date,
        v5.cost_type,
        v7.month_date_range,
        (v5.avg_costs_overall_per_day * v7.count_date_day_range) AS costs_overall_per_month
    FROM result_table_v5 AS v5
    LEFT JOIN result_table_v7 AS v7
        ON v5.ticket = v7.ticket
)

SELECT * FROM result_table_last_version
--WHERE ticket = 'OMFPKH-1409' -- period = 1 month
--WHERE ticket = 'OMFPKH-1223' -- period = 2 month
--WHERE ticket = 'OMFPKH-1216'
--WHERE ticket = 'OMFPKH-1406'
WHERE ticket = 'OMFPKH-61'
ORDER BY ticket, month_date_range






/* result_table_v4 AS(
    SELECT 
        ticket,
        start_date,
        end_date,
        cost_type,
        costs_overall,
        month_date_range,
        COUNT(date_day_range) OVER(PARTITION BY month_date_range ORDER BY date_day_range) AS date_day_range
        --SUM(costs_overall)/COUNT(ROW_NUMBER() OVER(PARTITION BY month_date_range ORDER BY ticket))
    FROM result_table_v3
), result_table_v5 AS(
    SELECT  
        ticket,
        start_date,
        end_date,
        cost_type,
        month_date_range,
        costs_overall,
        COUNT(date_day_range) AS count_date_day_range
    FROM result_table_v4
    GROUP BY 1,2,3,4,5,6
), result_table_v6 AS(
    SELECT DISTINCT 
        ticket,
        start_date,
        end_date,
        cost_type,
        month_date_range,
        (costs_overall/count_date_day_range) AS avg_costs_overall_per_day
    FROM result_table_v5
), result_table_v7 AS(
    SELECT 
        v6.ticket,
        v6.start_date,
        v6.end_date,
        v6.cost_type,
        v6.month_date_range,
        (v6.avg_costs_overall_per_day * v5.count_date_day_range) AS costs_overall_per_month
    FROM result_table_v6 AS v6
    LEFT JOIN result_table_v5 AS v5
        ON v6.ticket = v5.ticket
        AND v6.month_date_range = v5.month_date_range 
)
SELECT * FROM result_table_v4
--WHERE ticket = 'OMFPKH-1409' -- period = 1 month
WHERE ticket = 'OMFPKH-1223' -- period = 2 month
ORDER BY ticket --, date_day_range	
--, date_day_range*/

