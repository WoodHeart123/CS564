#!/bin/bash
rm *.dat
python3 skeleton_parser.py ebay_data/items-*.json
sqlite3 Auction.db < create.sql