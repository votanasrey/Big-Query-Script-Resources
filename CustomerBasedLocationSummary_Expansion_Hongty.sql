
DECLARE
  start_date, end_date DATE;
SET
  start_date = "2019-12-01";  --- edit start_date
SET
  end_date = CURRENT_DATE; 

WITH first_order AS
(
   SELECT
        LEFT(STRING(orders.created_date_local),7) as report_month,
        orders.created_at_utc,
        orders.pd_customer_uuid,
        orders.vendor_code,
        v.location.city as city_name,
        --COUNT(1), COUNT(DISTINCT orders.pd_customer_uuid)

    FROM
        `fulfillment-dwh-production.pandata_curated.pd_orders` AS orders
    LEFT JOIN 
        `fulfillment-dwh-production.pandata_curated.pd_vendors` AS v 
        ON orders.vendor_code = v.vendor_code 
        AND orders.global_entity_id = v.global_entity_id
    WHERE
        orders.created_date_utc >= start_date
        AND orders.created_date_utc <= end_date
        AND orders.created_date_local >= start_date
        AND orders.created_date_local < end_date
        AND orders.global_entity_id = "FP_KH"
        AND orders.is_valid_order
        AND NOT orders.is_test_order
    GROUP BY 1,2,3,4,5
    QUALIFY ROW_NUMBER() OVER (PARTITION BY pd_customer_uuid ORDER BY orders.created_at_utc ASC) = 1
)
, Active_Customer AS
(
    SELECT DISTINCT
        LEFT(STRING(orders.created_date_local),7) as report_month,
        orders.pd_customer_uuid,
        fo.city_name,

    FROM
        `fulfillment-dwh-production.pandata_curated.pd_orders` AS orders
    LEFT JOIN first_order as fo 
        ON orders.pd_customer_uuid = fo.pd_customer_uuid
    WHERE
        orders.created_date_utc >=  start_date
        AND orders.created_date_utc <= end_date
        AND orders.created_date_local >=  start_date
        AND orders.created_date_local < end_date
        AND orders.global_entity_id = "FP_KH"
        AND orders.is_valid_order
        AND NOT orders.is_test_order
), result as 
(SELECT report_month,city_name,COUNT(DISTINCT pd_customer_uuid) active_sub_base, 0 as new_customer_base
FROM Active_Customer
GROUP BY 1,2
UNION ALL
SELECT report_month,city_name,0,COUNT(DISTINCT pd_customer_uuid)
FROM first_order
GROUP BY 1,2)
, final as
(select report_month, 
city_name, 
SUM(new_customer_base) new_customer_base, 
SUM(active_sub_base) active_sub_base, 
--SUM() OVER (PARTITION BY report_month, city_name ORDER BY report_month DESC) total_customer_base,
from result
GROUP BY 1,2)
SELECT *, SUM(new_customer_base) over (PARTITION BY city_name order by report_month asc) as total_customer_base FROM final


