
DECLARE date1, date2 DATE;
SET date1 = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY);
SET date2 = DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY);
WITH google_analytic_vendor_session_table AS(
    SELECT 
        ga.global_entity_id,
        ga.pd_vendor_code, 
        ga.ga_vendor_id,
        ga.vendor_details.vendor_type,
        COUNT(DISTINCT ga.ga_session_id) AS total_sessions_visited,
        COUNT(ga.cart_visit) AS total_carts_visited,
        COUNT(ga.checkout_details.checkout_visit) AS total_checkout_visited,
        COUNT(ga.is_order IS TRUE) AS total_placed_orders,

        --COUNT(DISTINCT ga.vendor_details.is_favorite IS TRUE) AS total_favorites,
        --SUM(DISTINCT cart_details.item_added_to_cart) AS total_products_added_to_cart,
        --SUM(DISTINCT cart_details.item_removed_from_cart) AS total_products_removed_to_cart,
        
    FROM `fulfillment-dwh-production.pandata_curated.ga_vendors_sessions` AS ga
    LEFT JOIN UNNEST(ga.cart_details) AS cart_details
    WHERE 
        ga.global_entity_id IN ('FP_KH') 
        AND ga.is_active
        AND ga.partition_date >= date1
    GROUP BY 1,2,3,4
)
SELECT * FROM google_analytic_vendor_session_table ORDER BY total_sessions_visited DESC


