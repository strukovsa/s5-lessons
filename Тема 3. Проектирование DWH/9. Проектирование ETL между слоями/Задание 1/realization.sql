INSERT INTO cdm.dm_settlement_report (restaurant_id,
                                restaurant_name,
                                settlement_date,
                                orders_count,
                                orders_total_sum,
                                orders_bonus_payment_sum,
                                orders_bonus_granted_sum,
                                order_processing_fee,
                                restaurant_reward_sum)
SELECT o.restaurant_id,
       r.restaurant_name,
       t.date,
       COUNT(DISTINCT f.order_id),
       SUM(f.total_sum),
       SUM(f.bonus_payment),
       SUM(f.bonus_grant),
       0.25*SUM(f.total_sum),
       0.75*SUM(f.total_sum) - SUM(f.bonus_payment)
FROM dds.fct_product_sales f
JOIN dds.dm_orders o ON f.order_id=o.id
JOIN dds.dm_timestamps t ON o.timestamp_id=t.id
JOIN dds.dm_restaurants r ON o.restaurant_id=r.id
WHERE o.order_status = 'CLOSED'
GROUP BY o.restaurant_id, r.restaurant_name, t.date
ON CONFLICT (restaurant_id, settlement_date)
DO UPDATE SET orders_count = EXCLUDED.orders_count,
            orders_total_sum = EXCLUDED.orders_total_sum,
            orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
            orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
            order_processing_fee = EXCLUDED.order_processing_fee,
            restaurant_reward_sum = EXCLUDED.restaurant_reward_sum