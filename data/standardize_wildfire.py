import sys
import os
sys.path.append(os.getcwd())

import datetime as date
import pandas as pd
import const as const

# # #                             # # #
#                                     #
# This file is pending documentation! #
#                                     #
# # #                             # # #

fileIO = open("./data/Oregon_Fire_Record.csv", 'r')
contents = fileIO.readlines()

contents[0] = "Year,Month,Day,InitialLatitude,InitialLongitude,FoundLatitude,FoundLongitude,County\n"

for i in range(1, len(contents)):
  fields = contents[i].split(',')

  record_found_lng = fields[1]
  record_found_lat = fields[2]

  record_year, record_month, record_day = fields[3][:10].split('/')

  record_init_lat = fields[4]
  record_init_lng = fields[5]

  record_county = fields[6]

  contents[i] = ','.join([record_year, record_month, record_day, record_init_lat, record_init_lng, record_found_lat, record_found_lng, record_county])

fileIO = open("./data/Oregon_Fire_Record.csv", 'w')
fileIO.write(''.join(contents))
fileIO.close()