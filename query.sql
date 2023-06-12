-- 1
SELECT p.product_id, p.product_name
FROM Products p
JOIN Order_Details od ON p.product_id = od.product_id
GROUP BY p.product_id, p.product_name
HAVING COUNT(*) > 10;

--2 
SELECT user_id, product_name, buy_times
FROM (
    SELECT o.user_id, p.product_name, COUNT(*) AS buy_times,
        ROW_NUMBER() OVER (PARTITION BY o.user_id ORDER BY COUNT(*) DESC) AS rn
    FROM Orders o
    JOIN Order_Details od ON o.order_id = od.order_id
    JOIN Products p ON od.product_id = p.product_id
    GROUP BY o.user_id, p.product_name
    HAVING COUNT(DISTINCT o.order_id) >= 3
)
WHERE rn = 1
ORDER BY user_id, buy_times DESC;

--3
SELECT o.order_hour_of_day, p.product_name, COUNT(*) AS total_sales
FROM Orders o
JOIN Order_Details od ON o.order_id = od.order_id
JOIN Products p ON od.product_id = p.product_id
GROUP BY o.order_hour_of_day, p.product_name
HAVING COUNT(*) = (
    SELECT MAX(cnt)
    FROM (
        SELECT o2.order_hour_of_day, COUNT(*) AS cnt
        FROM Orders o2
        JOIN Order_Details od2 ON o2.order_id = od2.order_id
        JOIN Products p2 ON od2.product_id = p2.product_id
        GROUP BY o2.order_hour_of_day, p2.product_name
    ) sub
    WHERE o.order_hour_of_day = sub.order_hour_of_day
    GROUP BY sub.order_hour_of_day
)
ORDER BY o.order_hour_of_day, total_sales DESC;

-- 3.1
WITH max_counts AS (
    SELECT o.order_hour_of_day, p.product_name, COUNT(*) AS count
    FROM Orders o
    JOIN Order_Details od ON o.order_id = od.order_id
    JOIN Products p ON od.product_id = p.product_id
    GROUP BY o.order_hour_of_day, p.product_name
),
hourly_max_counts AS (
    SELECT order_hour_of_day, MAX(count) AS max_count
    FROM max_counts
    GROUP BY order_hour_of_day
)
SELECT mc.order_hour_of_day, mc.product_name, mc.count AS total_sales
FROM max_counts mc
JOIN hourly_max_counts hmc ON mc.order_hour_of_day = hmc.order_hour_of_day AND mc.count = hmc.max_count
ORDER BY mc.order_hour_of_day, mc.count DESC;