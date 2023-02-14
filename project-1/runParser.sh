#!/bin/bash
rm *.dat
python3 skeleton_parser.py ebay_data/items-*.json
sqlite Auction < creat.sql