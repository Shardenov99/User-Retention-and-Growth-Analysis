WITH cohort_sizes AS (
  SELECT
    DATE_TRUNC(first_join_date, MONTH) AS cohort_month,
    COUNT(DISTINCT customer_id) AS cohort_size
  FROM `capstone-project-472910.app_transaction_dataset.customer_cleaned`
  GROUP BY cohort_month
),

activity AS (
  SELECT
    DATE_TRUNC(c.first_join_date, MONTH) AS cohort_month,
    DATE_DIFF(DATE_TRUNC(DATE(t.created_at), MONTH),
              DATE_TRUNC(DATE(c.first_join_date), MONTH), MONTH) AS months_since_join,
    COUNT(DISTINCT c.customer_id) AS active_customers
  FROM `capstone-project-472910.app_transaction_dataset.customer_cleaned` c
  JOIN `capstone-project-472910.app_transaction_dataset.transactions_cleaned` t
    ON c.customer_id = t.customer_id
  WHERE t.payment_status = 'Success'
  GROUP BY cohort_month, months_since_join
)

SELECT
  cs.cohort_month,
  cs.cohort_size,                      
  SUM(IF(a.months_since_join = 0, a.active_customers, 0)) AS m0,  
  SUM(IF(a.months_since_join = 1, a.active_customers, 0)) AS m1,
  SUM(IF(a.months_since_join = 2, a.active_customers, 0)) AS m2,
  SUM(IF(a.months_since_join = 3, a.active_customers, 0)) AS m3,
  SUM(IF(a.months_since_join = 4, a.active_customers, 0)) AS m4,
  SUM(IF(a.months_since_join = 5, a.active_customers, 0)) AS m5,
  SUM(IF(a.months_since_join = 6, a.active_customers, 0)) AS m6,
  SUM(IF(a.months_since_join = 7, a.active_customers, 0)) AS m7,
  SUM(IF(a.months_since_join = 8, a.active_customers, 0)) AS m8,
  SUM(IF(a.months_since_join = 9, a.active_customers, 0)) AS m9,
  SUM(IF(a.months_since_join = 10, a.active_customers, 0)) AS m10,
  SUM(IF(a.months_since_join = 11, a.active_customers, 0)) AS m11,
  SUM(IF(a.months_since_join = 12, a.active_customers, 0)) AS m12
FROM cohort_sizes cs
LEFT JOIN activity a
  ON cs.cohort_month = a.cohort_month
GROUP BY cs.cohort_month, cs.cohort_size
ORDER BY cs.cohort_month;




