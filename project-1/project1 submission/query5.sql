WITH temp(ID) AS (SELECT DISTINCT User.UserID 
                  FROM User
                  JOIN Item ON Item.SellerID = User.UserID 
                  WHERE User.Rating > 1000)
SELECT COUNT(*) FROM temp;