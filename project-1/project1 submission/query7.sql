WITH temp(Name) AS (SELECT DISTINCT Category.NAME 
                    FROM Category 
                    JOIN Item ON Item.ItemID = Category.ItemID 
                    WHERE Item.Current > 100 AND Item.NumberOfBids > 0)
SELECT COUNT(*) FROM temp;