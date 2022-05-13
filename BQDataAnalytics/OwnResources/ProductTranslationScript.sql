

--------------------------------------- khmer translations ---------------------------------------
WITH khmer_tran AS(
WITH khmer_translation AS (
  WITH t_zh AS (
    SELECT
      t.global_entity_id,
      t.pd_object_id,
      t.object_attribute,
      t.object_text,
      ROW_NUMBER() OVER (PARTITION BY t.global_entity_id, t.pd_object_id, t.object_attribute ORDER BY t.updated_at_utc DESC) AS is_latest
    FROM
      `fulfillment-dwh-production.pandata_curated.pd_catalog_translations` AS t
    LEFT JOIN
      `fulfillment-dwh-production.pandata_curated.pd_languages` AS l
    ON
      l.global_entity_id = t.global_entity_id
      AND l.id = t.pd_language_id
      AND NOT l.is_deleted
      AND l.is_active
    WHERE
      1=1
      AND t.object_type = 'Products'
      AND t.object_attribute IN ('title',
        'description')
      AND l.title = 'Khmer'
      AND t.global_entity_id = 'FP_KH' 
  ) 
  SELECT * EXCEPT(is_latest) FROM t_zh WHERE 1=1 AND is_latest = 1 
  
  ),
  
  title AS(
  SELECT
    a.global_entity_id,
    a.pd_object_id AS product_id,
    a.object_text AS khmer_title
  FROM
    khmer_translation AS a
  WHERE
    a.object_attribute = 'title' 
  ),
  description AS(
  SELECT
    a.global_entity_id,
    a.pd_object_id AS product_id,
    a.object_text AS khmer_description
  FROM
    khmer_translation AS a
  WHERE
    a.object_attribute = 'description' 
  ), kh_translations AS(
    SELECT
      COALESCE(t.global_entity_id, d.global_entity_id) AS global_entity_id,
      COALESCE(t.product_id, d.product_id) AS product_id,
      t.khmer_title,
      d.khmer_description,
    FROM title AS t
    FULL OUTER JOIN description AS d
    ON t.product_id = d.product_id
)
  SELECT * FROM kh_translations
), 
--------------------------------------- english translations ---------------------------------------
english_tran AS(
  WITH eng_translation AS (
    WITH t_en AS (
      SELECT
        t.global_entity_id,
        t.pd_object_id,
        t.object_attribute,
        t.object_text,
        ROW_NUMBER() OVER (PARTITION BY t.global_entity_id, t.pd_object_id, t.object_attribute ORDER BY t.updated_at_utc DESC) AS is_latest
      FROM `fulfillment-dwh-production.pandata_curated.pd_catalog_translations` AS t
      LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_languages` AS l
            ON l.global_entity_id = t.global_entity_id
            AND l.id = t.pd_language_id
            AND NOT l.is_deleted
            AND l.is_active
      WHERE 
        1=1
        AND t.object_type = 'Products'
        AND t.object_attribute IN ('title','description')
        AND l.title = 'English' 
        AND t.global_entity_id = 'FP_KH'
    )
    
    SELECT * EXCEPT(is_latest) FROM  t_en WHERE 1=1 AND is_latest = 1  
  ),  title AS(
    SELECT
      a.global_entity_id,
      a.pd_object_id AS product_id,
      a.object_text AS english_title
    FROM
      eng_translation AS a
    WHERE
      a.object_attribute = 'title' 
    ),
    description AS(
    SELECT
      a.global_entity_id,
      a.pd_object_id AS product_id,
      a.object_text AS english_description
    FROM
      eng_translation AS a
    WHERE
      a.object_attribute = 'description' 
    ), eng_translations AS(
      SELECT
        COALESCE(t.global_entity_id, d.global_entity_id) AS global_entity_id,
        COALESCE(t.product_id, d.product_id) AS product_id,
        t.english_title,
        d.english_description,
      FROM title AS t
      FULL OUTER JOIN description AS d
      ON t.product_id = d.product_id
  )
  SELECT * FROM eng_translations
),
--------------------------------------- chinese translations ---------------------------------------
chinese_tran AS(
  WITH chinese_translation AS (
    WITH t_zh AS (
      SELECT
        t.global_entity_id,
        t.pd_object_id,
        t.object_attribute,
        t.object_text,
        ROW_NUMBER() OVER (PARTITION BY t.global_entity_id, t.pd_object_id, t.object_attribute ORDER BY t.updated_at_utc DESC) AS is_latest
      FROM `fulfillment-dwh-production.pandata_curated.pd_catalog_translations` AS t
      LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_languages` AS l
            ON l.global_entity_id = t.global_entity_id
            AND l.id = t.pd_language_id
            AND NOT l.is_deleted
            AND l.is_active
      WHERE 1=1
        AND t.object_type = 'Products'
        AND t.object_attribute IN ('title', 'description')
        AND l.title = 'Chinese'
        AND t.global_entity_id = 'FP_KH'
    )
      SELECT * except(is_latest) FROM  t_zh WHERE 1=1 AND is_latest = 1
    ),  title AS(
      SELECT
        a.global_entity_id,
        a.pd_object_id AS product_id,
        a.object_text AS chinese_title
      FROM
        chinese_translation AS a
      WHERE
        a.object_attribute = 'title' 
      ),
      description AS(
      SELECT
        a.global_entity_id,
        a.pd_object_id AS product_id,
        a.object_text AS chinese_description
      FROM
        chinese_translation AS a
      WHERE
        a.object_attribute = 'description' 
      ), ch_translations AS(
        SELECT
          COALESCE(t.global_entity_id, d.global_entity_id) AS global_entity_id,
          COALESCE(t.product_id, d.product_id) AS product_id,
          t.chinese_title,
          d.chinese_description,
        FROM title AS t
        FULL OUTER JOIN description AS d
        ON t.product_id = d.product_id
    )
    SELECT * FROM ch_translations
),
--------------------------------------- all translations ---------------------------------------
all_translations AS (
  SELECT DISTINCT
    COALESCE(e.global_entity_id, c.global_entity_id, k.global_entity_id) AS global_entity_id,
    COALESCE(e.product_id, c.product_id, k.product_id) AS product_id,
    e.english_title,
    c.chinese_title,
    k.khmer_title,

    e.english_description,
    c.chinese_description,
    k.khmer_description, 
  FROM english_tran AS e
  LEFT JOIN chinese_tran AS c
    ON e.global_entity_id = c.global_entity_id
    AND e.product_id = c.product_id 
  LEFT JOIN khmer_tran AS k
    ON e.global_entity_id = k.global_entity_id
    AND e.product_id = k.product_id   
  GROUP BY 1,2,3,4,5,6,7,8
), 
--------------------------------------- product translations ---------------------------------------
products_and_translations AS (
  SELECT
    products.product_created_at_local,
    products.global_entity_id,
    products.pd_menu_category_uuid,
    products.uuid AS product_uuid,
    products.id AS product_id,
    products.title AS default_title,
    english_title,
    chinese_title,
    khmer_title,
    products.description AS default_description,
    english_description,
    chinese_description,
    khmer_description,
    products.is_active,
    products.is_deleted,
  FROM `fulfillment-dwh-production.pandata_curated.pd_products` AS products
  LEFT JOIN all_translations
        ON products.global_entity_id = all_translations.global_entity_id
        AND products.id = all_translations.product_id 
  WHERE 1=1
    AND products.is_active
    AND NOT products.is_deleted
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
),
--------------------------------------- vendor table ---------------------------------------
vendor_product AS (
  SELECT 
    v.global_entity_id,
    v.global_vendor_id,
    v.vendor_code as vendor_code,
    v.name as vendor_name,
    menu_cat.uuid as menu_uuid
  FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` as v 
    LEFT JOIN UNNEST(v.menu_categories) as menu_cat
  WHERE global_entity_id = "FP_KH"
  AND v.is_active
  AND NOT v.is_test
  AND NOT v.is_private
), 
--------------------------------------- result table ---------------------------------------
result_table AS(
  SELECT DISTINCT 
    a.global_vendor_id,
    a.vendor_code, a.vendor_name, 
    b.*
  FROM vendor_product  as a
  LEFT JOIN products_and_translations as b 
    ON a.global_entity_id = b.global_entity_id
    AND a.menu_uuid = b.pd_menu_category_uuid
  WHERE a.global_entity_id = 'FP_KH' 
  AND a.global_vendor_id IN (
  'HKA9AA',
  'HKA9AD',
  'HKA9WP',
  'HKA9WV',
  'HW46A1',
  'HW46AP',
  'HW46A5',
  'HW46ZS',
  '4FCB86',
  'HK17KN',
  'HZ03M4',
  'HWK1T1',
  'HKRBCC',
  'HWZDTB',
  'HKRBJ5',
  'HW6CVS',
  'HW623C',
  'HZGHQ9',
  'HWD8NG',
  'HWAI6N',
  'HZXHUI',
  'HKR4W9',
  'HKPJAD',
  'HZ4XB0',
  'HZPPIU',
  'HWE5P9',
  'HKY0MT',
  'HWHG00',
  'HKTRWS',
  'HWH57L',
  'HWWP50',
  'HZJE5N',
  'HZKOLG',
  'HW45N3',
  'HKQJWC',
  '45HYJ7',
  'HW89WU',
  'HWAJQM',
  'HKVVRV',
  'HZ4FR2',
  'HZHPZZ',
  'HW8FI2',
  'HWACY3',
  'HWEBWD',
  'HZTP8K',
  'HW8KEP',
  'HW8K4R',
  'HZBK6Q',
  'HKYJGZ',
  'HZRPKH',
  'HWKXCC',
  'HZBEWI',
  'HZV7OK',
  'HWLM3E',
  'HWP4QT',
  'HZV7UM',
  'HWLSRH',
  'HWROK3',
  'HZ2ULY',
  'HW8HRA',
  'HWLM3R',
  'HZ31LV',
  'HK5G9E',
  'HKYMWG',
  'HZJFE9',
  'HZV751',
  'HKQ3HD',
  'HWAW49',
  'HK5G1T',
  'HZLFT2',
  'HKAOUM',
  'HZQ4LU',
  'HWL7J1',
  'HWRC8J',
  'HWKU7F',
  'HWZ1ZF',
  'HK9161',
  'HK1FIG',
  'HWRHJL',
  'HZLFPF',
  'HWE5LA',
  'HW4JLA',
  'HZ90FV',
  'HW6JQH',
  'HZQ8N7',
  'HZQ8SR',
  'HZHOKB',
  'HWAGI4',
  'HKV14G',
  'HZO6Y2',
  'HZ9E0R',
  'HWGG0Q',
  'HZ5W4Z',
  'HWG85W',
  'HZJGDQ',
  'HKPIUR',
  'HWG8YN',
  'HWRUML',
  'HWH5S3',
  'HWH5S7'
  )
  ORDER BY product_uuid DESC
) SELECT * FROM result_table 




