
DECLARE start_date, end_date date;
SET start_date = "2022-02-16";
SET end_date = "2022-02-28";

WITH CTE AS (
    SELECT orders.created_date_local,
        orders.vendor_code,
        orders.vendor_name,
        vendors.business_type_apac as type,
        orders.pd_customer_uuid,
        customers.code as customer_code,
        orders.code as order_code,
        products.title,
        products.pd_product_id,
        FROM `fulfillment-dwh-production.pandata_curated.pd_orders` as orders,
        UNNEST(products) as products
        JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_business_types` as vendors ON orders.vendor_code = vendors.vendor_code
        AND orders.global_entity_id = vendors.global_entity_id
        JOIN `fulfillment-dwh-production.pandata_curated.pd_customers` as customers ON orders.pd_customer_uuid = customers.uuid
        AND orders.global_entity_id = customers.global_entity_id
    WHERE orders.created_date_utc between DATE_SUB(start_date, INTERVAL 1 DAY)
        AND end_date
        AND orders.created_date_local between start_date AND end_Date
        AND orders.global_entity_id = "FP_KH"
        AND vendors.business_type_apac = "dmart"
        AND orders.is_valid_order
        AND products.title IN (
            "Chilli Sauce Leang Leng 200G",
            "Premium Fish Sauce 100% Trey Kanchanhchras 750Ml",
            "Premium Fish Sauce 100% Trey Kanchanhchras 700Ml",
            "Premium Fish Sauce 100% Trey Kanchanhchras 300Ml",
            "Golden Fresh Water Loster Yacht1(3Years) 750Ml",
            "Lim Kim Hong Yellow Vinegar 500Ml",
            "Oyster Sauce Leang Leng 700G",
            "Fresh Water Lobster Yacht 1 750Ml",
            "Golden Fresh Water Lobster Yacht 1 300Ml",
            "Golden Fresh Water Lobster Yacht1(3Years) 100Ml",
            "Salty Cashew Nut 170G",
            "Salty Cashew Nut 250G",
            "Vital Premium Water 500Mlx24'S",
            "Lor Laudry Detergent",
            "Chilli Sauce Special Leang Leng 500G",
            "Mee Chiet Chicken Soupx24'S",
            "Mee Chiet Minced Porkx24'S",
            "Fish Sauce Trey Kanchanhchras 450Ml",
            "Premium Fish Sauce 100% Trey Kanchanhchras 700Ml",
            "Fish Sauce Organic 750Ml",
            "Lor Dishwashing Liquid",
            "Mee Chiet Beef Stewx24'S",
            "Diabetasol Powder Chocolate Flavour 180G",
            "Lor Multi-Purpose Cleaner",
            "Lor Fabric Softener",
            "Vital Premium Water 1.5Lx12'S",
            "Vital Premium Water 350Mlx24'S",
            "Happy Hen Free Range Eggs 10'S"
        )
)
SELECT * FROM CTE