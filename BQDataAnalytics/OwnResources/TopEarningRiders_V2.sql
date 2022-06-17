
DECLARE
  start_date date DEFAULT DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH),  MONTH);
DECLARE
  end_date date DEFAULT CURRENT_DATE();

WITH
  basic AS(
  WITH
    pa AS (
    SELECT
      lg_country_code,
      lg_rider_id,
      basic.lg_payments_basic_rule_id AS lg_payments_basic_rule_id,
      created_date_local,
      basic.total_local AS total
    FROM
      `fulfillment-dwh-production.pandata_curated.lg_daily_rider_payments` AS lg_daily_rider_payments
    CROSS JOIN
      UNNEST (details.basic) AS basic
    WHERE
      lg_country_code IS NOT NULL
      AND lg_country_code IN ('kh')
      AND basic.status IN ('PAID',
        'PENDING')
      AND created_date_local BETWEEN start_date
      AND end_date),
    -- adjust start and end date
    ru AS (
    SELECT
      DISTINCT lg_country_code,
      id,
      type,
      sub_type
    FROM
      `fulfillment-dwh-production.pandata_curated.lg_payments_basic_rules` AS lg_payments_basic_rules)
  SELECT
    pa.lg_country_code,
    pa.lg_rider_id,
    pa.created_date_local,
    ru.type,
    ru.sub_type,
    SUM(pa.total) AS amount
  FROM
    pa
  LEFT JOIN
    ru
  ON
    pa.lg_country_code = ru.lg_country_code
    AND ru.id = pa.lg_payments_basic_rule_id
  WHERE
    pa.lg_country_code IN ('kh')
  GROUP BY
    1,
    2,
    3,
    4,
    5
  ORDER BY
    1,
    2,
    3,
    4,
    5 ),
  -- Rooster scoring payments extraction
  scoring AS (
  SELECT
    lg_country_code,
    lg_rider_id,
    DATE(scoring.created_at_local) AS created_date_local,
    AVG(scoring.scoring_amount) AS scoring_amount,
    SUM(scoring.total_local) AS Total_Scoring_Payment
  FROM
    `fulfillment-dwh-production.pandata_curated.lg_daily_rider_payments` AS lg_daily_rider_payments
  CROSS JOIN
    UNNEST (details.scoring) AS scoring
  WHERE
    lg_country_code IS NOT NULL
    AND lg_country_code IN ('kh')
    AND scoring.status IN ('PAID',
      'PENDING')
    AND DATE(scoring.created_at_local) BETWEEN start_date
    AND end_date -- adjust start and end date
    AND created_date_local BETWEEN start_date
    AND end_date -- adjust start and end date
  GROUP BY
    1,
    2,
    3),
  -- csv
  csvf AS (
  WITH
    csv AS(
    SELECT
      lg_country_code,
      lg_rider_id,
      DATE(sp.created_at_local) AS payment_created_date_local,
      sp.status,
      sp.adjustment_type,
      SUM(payment_local) AS csv_pay
    FROM
      `fulfillment-dwh-production.pandata_curated.lg_rider_special_payments`
    CROSS JOIN
      UNNEST (payment_details) AS sp
    WHERE
      lg_country_code IN ('kh')
      AND created_date_local BETWEEN start_date
      AND end_date
      AND sp.adjustment_type = 'FEE'
      AND sp.status IN ('PAID',
        'PENDING')
    GROUP BY
      1,
      2,
      3,
      4,
      5 ),
    rider_contract AS (
    SELECT
      DISTINCT lg_country_code,
      r.id AS rider_id,
      ct.lg_city_id AS city_id
    FROM
      `fulfillment-dwh-production.pandata_curated.lg_riders` AS r
    CROSS JOIN
      UNNEST (contracts) AS ct )
  SELECT
    csv.lg_country_code,
    csv.lg_rider_id,
    rider_contract.city_id,
    csv.payment_created_date_local,
    csv.status,
    csv.adjustment_type,
    csv.csv_pay
  FROM
    csv
  LEFT JOIN
    rider_contract
  ON
    csv.lg_country_code = rider_contract.lg_country_code
    AND csv.lg_rider_id = rider_contract.rider_id ),
  -- tip
  t AS (
  WITH
    tip AS (
    SELECT
      lg_country_code,
      lg_rider_id,
      DATE(rt.created_at_local) AS payment_created_date_local,
      rt.status,
      SUM(payment_local) AS tip_pay
    FROM
      `fulfillment-dwh-production.pandata_curated.lg_rider_tip_payments`
    CROSS JOIN
      UNNEST (payment_details) AS rt
    WHERE
      lg_country_code IN ('kh')
      AND created_date_local BETWEEN start_date
      AND end_date
      AND rt.status IN ('PAID',
        'PENDING')
    GROUP BY
      1,
      2,
      3,
      4 ),
    rider_contract AS (
    SELECT
      DISTINCT lg_country_code,
      r.id AS rider_id,
      ct.lg_city_id AS city_id
    FROM
      `fulfillment-dwh-production.pandata_curated.lg_riders` AS r
    CROSS JOIN
      UNNEST (contracts) AS ct )
  SELECT
    tip.lg_country_code,
    tip.lg_rider_id,
    rider_contract.city_id,
    tip.payment_created_date_local,
    tip.status,
    tip.tip_pay
  FROM
    tip
  LEFT JOIN
    rider_contract
  ON
    tip.lg_country_code = rider_contract.lg_country_code
    AND tip.lg_rider_id = rider_contract.rider_id ),
  -- deliveries completed
  corders AS (
  SELECT
    rider.lg_country_code,
    deliveries.lg_rider_id,
    DATE(deliveries.rider_accepted_at_local) AS rider_accepted_date_local,
    COUNT(deliveries.id) AS completed_deliveries,
    SUM(deliveries.pickup_distance_manhattan_in_meters)/1000 AS pickup_distance_meters,
    SUM(deliveries.dropoff_distance_manhattan_in_meters)/1000 AS dropoff_distance_meters
  FROM
    `fulfillment-dwh-production.pandata_curated.lg_orders` AS lg_orders
  CROSS JOIN
    UNNEST (rider.deliveries) AS deliveries
  WHERE
    deliveries.status = 'completed'
    AND rider.lg_country_code IS NOT NULL
    AND rider.lg_country_code NOT IN ('dp-sg')
    AND rider.lg_country_code = "kh"
    AND DATE(deliveries.rider_accepted_at_local) BETWEEN start_date
    AND end_date -- adjust start and end date
    AND lg_orders.created_date_utc >= start_date -- adjust start and end date
    --AND deliveries.lg_rider_id = 231299
  GROUP BY
    1,
    2,
    3),
  rider_city AS(
  WITH
    rider_city_id AS (
    SELECT
      DISTINCT lg_country_code,
      r.id AS rider_id,
      c.lg_city_id,
      c.start_at_local AS contract_start
    FROM
      `fulfillment-dwh-production.pandata_curated.lg_riders` AS r
    CROSS JOIN
      UNNEST (contracts) AS c
    WHERE
      lg_country_code IN ('kh')
      AND c.status = 'VALID' ),
    rider_latest_contract AS (
    SELECT
      DISTINCT lg_country_code,
      r.id AS rider_id,
      MAX(c.start_at_local) AS contract_start,
      r.batch_number
    FROM
      `fulfillment-dwh-production.pandata_curated.lg_riders` AS r
    CROSS JOIN
      UNNEST (contracts) AS c
    WHERE
      lg_country_code IN ('kh')
      AND c.status = 'VALID'
    GROUP BY
      1,
      2,
      4 ),
    city AS (
    SELECT
      DISTINCT country_code,
      cities.id AS city_id,
      cities.name AS city_name
    FROM
      `fulfillment-dwh-production.pandata_curated.lg_countries` AS c
    CROSS JOIN
      UNNEST (cities) AS cities
    WHERE
      country_code IN ('kh')
      AND cities.is_active )
  SELECT
    rider_latest_contract.lg_country_code,
    rider_latest_contract.rider_id,
    rider_city_id.lg_city_id AS city_id,
    city.city_name,
    rider_latest_contract.batch_number
  FROM
    rider_latest_contract
  LEFT JOIN
    rider_city_id
  ON
    rider_latest_contract.lg_country_code = rider_city_id.lg_country_code
    AND rider_latest_contract.rider_id = rider_city_id.rider_id
    AND rider_latest_contract.contract_start = rider_city_id.contract_start
  LEFT JOIN
    city
  ON
    rider_city_id.lg_country_code = city.country_code
    AND rider_city_id.lg_city_id = city.city_id ),
  shift AS (
  SELECT
    lg_country_code,
    DATE(actual_start_at_local) shift_date_local,
    lg_rider_id,
    COUNT(DISTINCT DATE(actual_start_at_local)) AS shift_day_count,
    COUNT(DISTINCT uuid) shift_count,
    SUM(actual_working_time_in_seconds)/3600 AS shift_hours
  FROM
    `fulfillment-dwh-production.pandata_curated.lg_shifts` AS sh
  WHERE
    DATE(actual_start_at_local) BETWEEN start_date
    AND end_date
    AND created_date_utc BETWEEN DATE_SUB(start_date, INTERVAL 1 WEEK)
    AND end_date
    AND lg_country_code = "kh"
  GROUP BY
    1,
    2,
    3 ),
  final AS (
  SELECT
    basic.lg_country_code,
    basic.lg_rider_id,
    batch_number,
    rider_city.city_name,
    basic.created_date_local,
    completed_deliveries,
    scoring.scoring_amount,
    dropoff_distance_meters + pickup_distance_meters AS distance_km,
    IFNULL(Total_Scoring_Payment,
      0) AS Total_Scoring_Payment,
    IFNULL(csv_pay,
      0) AS Total_CSV_Payment,
    IFNULL(tip_pay,
      0) AS Total_Tip_Payment,
    shift_day_count,
    shift_count AS shift_count,
    shift_hours AS shift_hours,
    SUM (CASE
        WHEN basic.type = 'MINIMUM_PER_HOUR' THEN basic.amount
      ELSE
      NULL
    END
      ) AS minimum_guaranteed,
    SUM (CASE
        WHEN basic.type = 'PER_DELIVERY' THEN basic.amount
      ELSE
      NULL
    END
      ) AS order_component,
    SUM (CASE
        WHEN basic.type = 'PER_HOUR' THEN basic.amount
      ELSE
      NULL
    END
      ) AS hour_component,
    SUM (CASE
        WHEN basic.type = 'PER_KM' THEN basic.amount
      ELSE
      NULL
    END
      ) AS distance_component,
    SUM (CASE
        WHEN basic.type IS NULL THEN basic.amount
      ELSE
      NULL
    END
      ) AS fallback_pay
  FROM
    basic
  LEFT JOIN
    scoring
  ON
    basic.lg_country_code = scoring.lg_country_code
    AND basic.created_date_local = scoring.created_date_local
    AND basic.lg_rider_id = scoring.lg_rider_id
  LEFT JOIN
    csvf
  ON
    basic.lg_country_code = csvf.lg_country_code
    AND basic.created_date_local = csvf.payment_created_date_local
    AND basic.lg_rider_id = csvf.lg_rider_id
  LEFT JOIN
    t
  ON
    basic.lg_country_code = t.lg_country_code
    AND basic.created_date_local = t.payment_created_date_local
    AND basic.lg_rider_id = t.lg_rider_id
  LEFT JOIN
    corders
  ON
    basic.lg_country_code = corders.lg_country_code
    AND basic.created_date_local = corders.rider_accepted_date_local
    AND basic.lg_rider_id = corders.lg_rider_id
  LEFT JOIN
    shift
  ON
    basic.lg_rider_id = shift.lg_rider_id
    AND basic.created_date_local = shift.shift_date_local
    AND basic.lg_country_code = shift.lg_country_code
  LEFT JOIN
    rider_city
  ON
    basic.lg_country_code = rider_city.lg_country_code
    AND basic.lg_rider_id = rider_city.rider_id
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14 ),
  result_table AS(
  SELECT
    lg_country_code,
    lg_rider_id,
    city_name,
    IFNULL(SUM(IFNULL(minimum_guaranteed,
          0) + IFNULL(order_component,
          0) + IFNULL(hour_component,
          0) + IFNULL(distance_component,
          0) + IFNULL(fallback_pay,
          0) + IFNULL(Total_Scoring_Payment,
          0) + IFNULL(Total_CSV_Payment,
          0) + IFNULL(Total_Tip_Payment,
          0)),
      0) AS total_rider_payment,
    IFNULL(SUM(order_component),
      0) AS rider_order_component,
    IFNULL(SUM(hour_component),
      0) AS rider_hour_component,
    IFNULL(SUM(distance_component),
      0) AS rider_distance_component,
    IFNULL(SUM(fallback_pay),
      0) AS rider_fallback_component,
    IFNULL(SUM(Total_Scoring_Payment),
      0) AS rider_scoring_pay,
    IFNULL(SUM(Total_CSV_Payment),
      0) AS rider_csv_pay,
    IFNULL(SUM(Total_Tip_Payment),
      0) AS rider_tip_pay,
    IFNULL(SUM(completed_deliveries),
      0) AS rider_deliveries_completed,
    IFNULL(SUM(distance_km),
      0) AS distance_km,
    IFNULL(SUM(shift_hours),
      0) AS shift_hours
  FROM
    final
  WHERE
    lg_country_code NOT IN ('dp-sg')
    AND lg_country_code IN ('kh')
    AND lg_rider_id IS NOT NULL
    AND city_name = "Phnom penh"
  GROUP BY
    1,
    2,
    3
  ORDER BY
    total_rider_payment DESC )
SELECT
  *
FROM
  result_table


