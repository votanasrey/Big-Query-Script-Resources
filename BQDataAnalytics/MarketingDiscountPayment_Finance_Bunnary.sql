

WITH marketing_discount_cost_v1 AS(
    SELECT 
        pd_discount_uuid,
        discount_description,
        discount_type,
        budget_channel,
        vertical_per_budget,
        start_date,
        IF(end_date > DATE_ADD(DATE(last_updated_at_utc), INTERVAL 1 DAY), DATE_ADD(DATE(last_updated_at_utc), INTERVAL 1 DAY), DATE(end_date)) AS end_date,
        last_updated_at_utc,
        marketing_share,
        ratio_foodpanda,
        ROUND(SUM(costs_vendor_lc),2) AS costs_vendor_lc,
        ROUND(SUM(costs_fp_lc),2) AS costs_fp_lc
    FROM `fulfillment-dwh-production.pandata_report.marketing_cost_report_discounts_v2`  
    WHERE 
        global_entity_id = 'FP_KH'
    GROUP BY 1,2,3,4,5,6,7,8,9,10
), marketing_discount_cost_v2 AS(
    SELECT 
        pd_discount_uuid,
        discount_description,
        discount_type,
        budget_channel,
        vertical_per_budget,
        start_date,
        end_date,
        last_updated_at_utc,
        marketing_share,
        ratio_foodpanda,
        GENERATE_DATE_ARRAY(DATE(start_date), DATE(end_date),INTERVAL 1 DAY) AS date_range,
        costs_vendor_lc,
        costs_fp_lc
    FROM marketing_discount_cost_v1
), marketing_discount_cost_v3 AS(
    SELECT * EXCEPT(date_range) 
    FROM marketing_discount_cost_v2,
    UNNEST(date_range) AS date_day_range
), marketing_discount_cost_v4 AS(
    SELECT DISTINCT
        pd_discount_uuid,
        discount_description,
        discount_type,
        budget_channel,
        vertical_per_budget,
        start_date,
        end_date,
        date_day_range,
        SUBSTRING(STRING(DATE(date_day_range)),1,7) AS month_date_range,
        last_updated_at_utc,
        marketing_share,
        ratio_foodpanda,
        costs_vendor_lc,
        costs_fp_lc
    FROM marketing_discount_cost_v3
), marketing_discount_cost_v5 AS(
    SELECT 
        pd_discount_uuid,
        discount_description,
        discount_type,
        budget_channel,
        vertical_per_budget,
        start_date,
        end_date,
        last_updated_at_utc,
        marketing_share,
        ratio_foodpanda,
        costs_vendor_lc,
        costs_fp_lc,
        COUNT(DISTINCT date_day_range) AS number_of_day
    FROM marketing_discount_cost_v4
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
), marketing_discount_cost_v6 AS(
    SELECT 
        pd_discount_uuid,
        discount_description,
        discount_type,
        budget_channel,
        vertical_per_budget,
        start_date,
        end_date,
        last_updated_at_utc,
        marketing_share,
        ratio_foodpanda,
        (costs_vendor_lc/number_of_day) AS avg_costs_vendor_lc_per_day,
        (costs_fp_lc/number_of_day) AS avg_costs_fp_lc_per_day
    FROM marketing_discount_cost_v5
), marketing_discount_cost_v7 AS(
    SELECT 
        pd_discount_uuid,
        discount_description,
        discount_type,
        budget_channel,
        vertical_per_budget,
        start_date,
        end_date,
        month_date_range,
        last_updated_at_utc,
        marketing_share,
        ratio_foodpanda,
        COUNT(date_day_range) OVER(PARTITION BY month_date_range ORDER BY date_day_range) AS date_day_range
    FROM marketing_discount_cost_v4
), marketing_discount_cost_v8 AS(
    SELECT 
        pd_discount_uuid,
        discount_description,
        discount_type,
        budget_channel,
        vertical_per_budget,
        start_date,
        end_date,
        month_date_range,
        last_updated_at_utc,
        marketing_share,
        ratio_foodpanda,
        COUNT(DISTINCT date_day_range) AS count_date_day_range
    FROM marketing_discount_cost_v7
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
), result_table_1 AS (
    SELECT DISTINCT
        v8.pd_discount_uuid,
        v8.discount_description,
        v8.start_date,
        v8.end_date,
        v8.month_date_range,
        v8.last_updated_at_utc,
        ROUND((v6.avg_costs_fp_lc_per_day * v8.count_date_day_range),2) AS costs_fp_lc_per_month,
        ROUND((v6.avg_costs_vendor_lc_per_day * v8.count_date_day_range),2) AS costs_vendor_lc_per_month 
    FROM marketing_discount_cost_v8 AS v8
    LEFT JOIN marketing_discount_cost_v6 AS v6
        ON v8.pd_discount_uuid = v6.pd_discount_uuid
), result_table AS(
    SELECT DISTINCT
        pd_discount_uuid,
        discount_description,
        start_date,
        end_date,
        month_date_range,
        last_updated_at_utc,
        
        SUM(costs_fp_lc_per_month) AS costs_fp_lc_per_month,
        SUM(costs_vendor_lc_per_month) AS costs_vendor_lc_per_month
    FROM result_table_1 
    GROUP BY 1,2,3,4,5,6 
)
SELECT * FROM result_table 


-- WHERE pd_discount_uuid = 'OFRD_317102_FP_KH' -- check this result
-- WHERE pd_discount_uuid = 'OFRD_15522_FP_KH'  -- check this result, the costs are equal
-- WHERE pd_discount_uuid = 'OFRD_301066_FP_KH' -- check this result 
--WHERE pd_discount_uuid = 'OFRD_11018_FP_KH'  -- expected result
-- WHERE pd_discount_uuid = 'OFRD_12373_FP_KH'  -- expected result
-- WHERE pd_discount_uuid = 'OFRD_12413_FP_KH'  -- expected result
-- WHERE pd_discount_uuid = 'OFRD_302024_FP_KH' -- expected result
-- WHERE pd_discount_uuid IN (SELECT pd_discount_uuid FROM result_table GROUP BY 1 HAVING COUNT(1)>1)

ORDER BY pd_discount_uuid


