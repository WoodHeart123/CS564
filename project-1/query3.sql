WITH temp(count) AS (SELECT COUNT(Category.NAME) FROM Category GROUP BY Category.ItemID HAVING COUNT(Category.NAME) = 4)
SELECT COUNT(*) from temp;