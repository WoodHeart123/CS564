WITH temp(Cnt) AS (SELECT COUNT(Category.NAME) -- count is reserved? using Cnt
                   FROM Category 
                   GROUP BY Category.ItemID 
                   HAVING COUNT(DISTINCT Category.NAME) = 4) 
                   /* Add DISTINCT to match expect result*/
SELECT COUNT(*) 
FROM temp;