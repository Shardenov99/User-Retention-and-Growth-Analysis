
WITH unique_events AS (
  SELECT
    event_id,
    traffic_source,
    event_name,
    payment_status
  FROM `capstone-project-472910.app_transaction_dataset.clickstream_cleaned`
  WHERE event_name IN ('HOMEPAGE','SCROLL','ADD_TO_CART',"ITEM_DETAIL",'BOOKING')
  GROUP BY event_id, traffic_source, event_name, payment_status
),

user_journey AS (
  SELECT
    event_id,
    traffic_source,
    MAX(CASE WHEN event_name = 'HOMEPAGE' THEN 1 ELSE 0 END) AS homepage,
    MAX(CASE WHEN event_name = 'SCROLL' THEN 1 ELSE 0 END) AS scroll,
    MAX(CASE WHEN event_name = 'ADD_TO_CART' THEN 1 ELSE 0 END) AS add_to_cart,
    MAX(CASE WHEN event_name = 'ITEM_DETAIL' THEN 1 ELSE 0 END) AS item_detail,
    MAX(CASE WHEN event_name = 'BOOKING' AND payment_status = 'Success' THEN 1 ELSE 0 END) AS booking
  FROM unique_events
  GROUP BY event_id, traffic_source
)

SELECT
  traffic_source,
  SUM(homepage) AS homepage_sessions,
  SUM(scroll) AS scroll_sessions,
  SUM(add_to_cart) AS add_to_cart_sessions,
   SUM(item_detail) AS item_detail_sessions,
  SUM(booking) AS booking_sessions,
  ROUND(SUM(scroll)*100.0/SUM(homepage),2) AS pct_scroll,
  ROUND(SUM(add_to_cart)*100.0/SUM(homepage),2) AS pct_add_to_cart,
   ROUND(SUM(item_detail)*100.0/SUM(homepage),2) AS pct_item_detail,
  ROUND(SUM(booking)*100.0/SUM(homepage),2) AS pct_booking
FROM user_journey
GROUP BY traffic_source
ORDER BY homepage_sessions DESC;

