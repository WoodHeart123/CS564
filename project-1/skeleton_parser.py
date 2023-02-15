
"""
FILE: skeleton_parser.py
------------------
Author: Firas Abuzaid (fabuzaid@stanford.edu)
Author: Perth Charernwattanagul (puch@stanford.edu)
Modified: 04/21/2014

Skeleton parser for CS564 programming project 1. Has useful imports and
functions for parsing, including:

1) Directory handling -- the parser takes a list of eBay json files
and opens each file inside of a loop. You just need to fill in the rest.
2) Dollar value conversions -- the json files store dollar value amounts in
a string like $3,453.23 -- we provide a function to convert it to a string
like XXXXX.xx.
3) Date/time conversions -- the json files store dates/ times in the form
Mon-DD-YY HH:MM:SS -- we wrote a function (transformDttm) that converts to the
for YYYY-MM-DD HH:MM:SS, which will sort chronologically in SQL.

Your job is to implement the parseJson function, which is invoked on each file by
the main function. We create the initial Python dictionary object of items for
you; the rest is up to you!
Happy parsing!
"""

import sys
from json import loads
from re import sub

columnSeparator = "|"
userIDDict = {}

# Dictionary of months used for date transformation
MONTHS = {'Jan':'01','Feb':'02','Mar':'03','Apr':'04','May':'05','Jun':'06',\
        'Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}

def escape(str):
  return "\"" + str.replace("\"","\"\"") + "\""
  
"""
Returns true if a file ends in .json
"""
def isJson(f):
    return len(f) > 5 and f[-5:] == '.json'

"""
Converts month to a number, e.g. 'Dec' to '12'
"""
def transformMonth(mon):
    if mon in MONTHS:
        return MONTHS[mon]
    else:
        return mon

"""
Transforms a timestamp from Mon-DD-YY HH:MM:SS to YYYY-MM-DD HH:MM:SS
"""
def transformDttm(dttm):
    dttm = dttm.strip().split(' ')
    dt = dttm[0].split('-')
    date = '20' + dt[2] + '-'
    date += transformMonth(dt[0]) + '-' + dt[1]
    return date + ' ' + dttm[1]

"""
Transform a dollar value amount from a string like $3,453.23 to XXXXX.xx
"""

def transformDollar(money):
    if money == None or len(money) == 0:
        return money
    return sub(r'[^\d.]', '', money)

"""
Parses a single json file. Currently, there's a loop that iterates over each
item in the data set. Your job is to extend this functionality to create all
of the necessary SQL tables for your database.
"""
def parseJson(json_file):
    global userIDDict
    categoryList, itemList, bidList = [],[],[]
    with open(json_file, 'r') as f:
        items = loads(f.read())['Items'] # creates a Python dictionary of Items for the supplied json file
        for item in items:

            for category in item["Category"]:
                categoryList.append([item["ItemID"], category])
            # traverse bids if exist
            if item["Bids"] != None:
                for bid in item["Bids"]:
                    bidList.append([item["ItemID"],"\"" + bid["Bid"]["Bidder"]["UserID"] + "\"", transformDttm(bid["Bid"]["Time"]), transformDollar(bid["Bid"]["Amount"])])
                
                    if bid["Bid"]["Bidder"]["UserID"] not in userIDDict.keys():
                        bidder = bid["Bid"]["Bidder"]
                        userIDDict[bidder["UserID"]] = ["\"" + bidder["UserID"] + "\"", bidder["Rating"], escape(bidder.get("Location","NULL")), escape(bidder.get("Country","NULL"))]
            # check if seller info is in the list
            if item["Seller"]["UserID"] not in userIDDict.keys():
                userIDDict[item["Seller"]["UserID"]] = [escape(item["Seller"]["UserID"]) , item["Seller"]["Rating"], escape(item["Location"]), escape(item["Country"])]

            # add item 
            if item["Description"] != None:
                itemList.append([item["ItemID"], escape(item["Seller"]["UserID"]), escape(item["Name"]),
                            transformDollar(item["Currently"]), transformDollar(item["First_Bid"]), item["Number_of_Bids"], 
                            transformDttm(item["Started"]), transformDttm(item["Ends"]), escape(item["Description"]), 
                            item.get("Buy_Price","NULL")])

    # create load files    
    createLoadFile(itemList, "item.dat")
    createLoadFile(bidList, "bid.dat")
    createLoadFile(categoryList, "category.dat")

def createLoadFile(data, filename, separater="|"):
    with open(filename, "a+") as f:
        for row in data:
            f.write(separater.join(row) + '\n')


"""
Loops through each json files provided on the command line and passes each file
to the parser
"""
def main(argv):
    global userIDDict
    if len(argv) < 2:
        print("Usage: python skeleton_json_parser.py <path to json files>",file=sys.stderr)
        sys.exit(1)
    # loops over all .json files in the argument
    for f in argv[1:]:
        if isJson(f):
            parseJson(f)
            print("Success parsing ",f)
    createLoadFile(userIDDict.values(), "user.dat")

if __name__ == '__main__':
    main(sys.argv)
