WITH array_con AS (
  SELECT id, json_array_elements((event_value::JSON->>'product_payments')::JSON) AS product_payment
  FROM public.outbox o 
)
SELECT DISTINCT product_payment->>'product_name' AS product_name
FROM array_con;