WITH temp(ID) AS (SELECT DISTINCT User.UserID 
                  FROM User,Item,Bid 
                  WHERE User.UserID = Item.SellerID AND 
                  			Item.SellerID = Bid.BidderID)
SELECT COUNT(*) FROM temp;