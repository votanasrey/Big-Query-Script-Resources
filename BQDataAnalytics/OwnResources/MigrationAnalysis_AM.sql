
DECLARE pre_start_date, pre_end_date, post_start_date, post_end_date DATE;
DECLARE keyword STRING;

SET pre_start_date = "2022-02-01"; --Main comparison part
SET pre_end_date = "2022-02-28";

SET post_start_date = "2022-03-01"; -- Which date you want to compare to
SET post_end_date = "2022-03-31";

SET keyword = "koi"; --keyword on the vendor name

WITH 

pre_list AS 
(
  SELECT
        pd_customer_uuid,
    FROM
        `fulfillment-dwh-production.pandata_curated.pd_orders` AS orders
    WHERE
        orders.created_date_utc BETWEEN pre_start_date AND DATE_ADD(pre_end_date, INTERVAL 1 DAY)
        AND orders.created_date_local BETWEEN pre_start_date AND pre_end_date
        AND orders.global_entity_id = 'FP_KH'
        AND is_valid_order
        AND LOWER(vendor_name) like CONCAT("%",keyword,"%")
  GROUP BY 1
)

, post_list AS 
(
  SELECT
        pd_customer_uuid,
    FROM
        `fulfillment-dwh-production.pandata_curated.pd_orders` AS orders
    WHERE
        orders.created_date_utc BETWEEN post_start_date AND DATE_ADD(post_end_date, INTERVAL 1 DAY)
        AND orders.created_date_local BETWEEN post_start_date AND post_end_date
        AND orders.global_entity_id = 'FP_KH'
        AND is_valid_order
        AND LOWER(vendor_name) like CONCAT("%",keyword,"%")
  GROUP BY 1
)

, migration_list AS 
(
  SELECT pre_list.pd_customer_uuid 
  FROM pre_list 
  LEFT JOIN post_list 
  ON pre_list.pd_customer_uuid = post_list.pd_customer_uuid
  WHERE post_list.pd_customer_uuid IS NULL
)

, pre AS 
(
  SELECT
        --DATE_TRUNC(orders.created_date_local, MONTH) AS months,
        vendor_code, 
        pd_customer_uuid,
        SUM(CASE WHEN orders.is_valid_order THEN accounting.gfv_local END) AS total_gfv_local,
        COUNT(DISTINCT orders.code) AS total_orders,
        COUNT(CASE WHEN orders.is_valid_order THEN orders.code END) AS total_valid_orders,
        COUNT(CASE WHEN NOT orders.is_valid_order IS FALSE THEN orders.code END) AS total_failed_orders, 
        --STRING_AGG(vendor_code, ", ") as vendor_code_list
    FROM
        `fulfillment-dwh-production.pandata_curated.pd_orders` AS orders
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` AS accounting
       ON orders.uuid = accounting.uuid
    WHERE
        orders.created_date_utc BETWEEN pre_start_date AND DATE_ADD(pre_end_date, INTERVAL 1 DAY)
        AND orders.created_date_local BETWEEN pre_start_date AND pre_end_date
        AND accounting.created_date_utc BETWEEN pre_start_date AND DATE_ADD(pre_end_date, INTERVAL 1 DAY)
        AND orders.global_entity_id = 'FP_KH'
        AND orders.is_test_order IS FALSE
        AND pd_customer_uuid IN (SELECT pd_customer_uuid FROM migration_list)
        --AND LOWER(vendor_name) like CONCAT("%",keyword,"%")
        --AND orders.
    GROUP BY 1,2
)
, post AS
(
  SELECT
        vendor_code,
        vendor_name,
        pd_customer_uuid,
        SUM(CASE WHEN orders.is_valid_order THEN accounting.gfv_local END) AS total_gfv_local,
        COUNT(DISTINCT orders.code) AS total_orders,
        COUNT(CASE WHEN orders.is_valid_order THEN orders.code END) AS total_valid_orders,
        COUNT(CASE WHEN NOT orders.is_valid_order THEN orders.code END) AS total_failed_orders, 
    FROM
        `fulfillment-dwh-production.pandata_curated.pd_orders` AS orders
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` AS accounting
       ON orders.uuid = accounting.uuid
    WHERE
        orders.created_date_utc BETWEEN post_start_date AND DATE_ADD(post_end_date, INTERVAL 1 DAY)
        AND orders.created_date_local BETWEEN post_start_date AND post_end_date
        AND accounting.created_date_utc BETWEEN post_start_date AND DATE_ADD(post_end_date, INTERVAL 1 DAY)
        AND orders.global_entity_id = 'FP_KH'
        AND orders.is_test_order IS FALSE
        AND pd_customer_uuid IN (SELECT pd_customer_uuid FROM migration_list)
    GROUP BY 1,2,3
)
SELECT 
"Pre Period" AS Period,
COUNT(DISTINCT pd_customer_uuid) AS Customer_count,
SUM(total_valid_orders) AS Valid_orders,
SUM(total_gfv_local)/SUM(total_valid_orders) AS AFV,
(SUM(total_valid_orders)/COUNT(DISTINCT pd_customer_uuid)) as Frequency,
--(SELECT STRING_AGG(DISTINCT vendor_code, ", ") FROM pre) as Vendor_list,
NULL as Vendor,
NULL as Order_count,
NULL as GFV,
FROM pre
UNION ALL
SELECT  
"Post Period" AS Period,
COUNT(DISTINCT pd_customer_uuid) AS Customer_count,
SUM(total_valid_orders) AS Valid_orders,
SUM(total_gfv_local)/SUM(total_valid_orders) AS AFV,
(SUM(total_valid_orders)/COUNT(DISTINCT pd_customer_uuid)) as Frequency,
--NULL--(SELECT STRING_AGG(DISTINCT vendor_code, "") FROM pre) as Vendor_list,
NULL,
NULL,
NULL
FROM post 
UNION ALL
SELECT *
FROM
(SELECT 
"Post Period Vendor",  
NULL,
NULL,
NULL,
NULL,

CONCAT(vendor_code,": ",vendor_name) AS Vendor,
SUM(total_valid_orders) as Order_count,
SUM(total_gfv_local) as GFV,
FROM post
GROUP BY 6
ORDER BY 7 DESC
LIMIT 20) AS Vendor
ORDER BY (CASE WHEN Period = "Pre Period" THEN 1 WHEN Period = "Post Period" THEN 2 WHEN Period = "Post Period Vendor" THEN 3 END) ASC, Order_count DESC

