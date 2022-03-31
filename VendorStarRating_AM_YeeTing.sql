

DECLARE period_date DATE;
SET period_date = DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH);
WITH vendor_table AS(
    SELECT 
        vendor.global_entity_id,
        vendor.vendor_code,
        vendor.name AS vendor_name
    FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` AS vendor
    WHERE 
        vendor.global_entity_id = 'FP_KH'
        AND vendor.is_active
        AND NOT vendor.is_test
        AND NOT vendor.is_private
), vendor_rating_table AS(
    SELECT 
        review.global_entity_id,
        review.data_stream_vendor_id AS vendor_code,
        review.ratings.vendor_food.score AS vendor_food_score_rating,
        review.ratings.rider.score AS rider_score_rating,
        review.ratings.packaging.score AS packaging_score_rating,
        --ROUND(review.avg_score,1) AS avg_score,
        ROW_NUMBER() OVER (PARTITION BY review.data_stream_order_id ORDER BY review.created_date_utc) AS rating_rank
    FROM `fulfillment-dwh-production.pandata_curated.data_stream_reviews` AS review
    WHERE
        review.global_entity_id = 'FP_KH'
        AND DATE(review.created_date_utc) >= period_date
        AND DATE(review.updated_date_utc) >= period_date
), result_table AS(
    SELECT 
        vendor_table.vendor_code,
        vendor_table.vendor_name,
        ROUND(SUM(vendor_rating_table.vendor_food_score_rating)/COUNT(CASE WHEN vendor_rating_table.vendor_food_score_rating 
            IS NOT NULL THEN vendor_rating_table.vendor_food_score_rating END),1) AS vendor_food_score_rating,
        --ROUND(SUM(vendor_rating_table.rider_score_rating)/COUNT(CASE WHEN vendor_rating_table.rider_score_rating 
        --    IS NOT NULL THEN vendor_rating_table.rider_score_rating END),1) AS rider_score_rating,
        --ROUND(SUM(vendor_rating_table.packaging_score_rating)/COUNT(CASE WHEN vendor_rating_table.packaging_score_rating 
        --    IS NOT NULL THEN vendor_rating_table.packaging_score_rating END),1) AS packaging_score_rating

    FROM vendor_table 
    INNER JOIN vendor_rating_table
        ON vendor_table.vendor_code = vendor_rating_table.vendor_code
        AND vendor_table.global_entity_id = vendor_rating_table.global_entity_id
    WHERE vendor_rating_table.rating_rank = 1
    GROUP BY 1,2
)
SELECT * FROM result_table

