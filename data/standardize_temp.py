import sys
import os
sys.path.append(os.getcwd())

import datetime as date
import pandas as pd
import const as const

# # #                        # # #
#                                #
# This file is tagged: [DEFUNCT] #
#                                #
# # #                        # # #

for county in const.county_list:
  fileIO = open("./data/Temperature_By_County/{}_Temp.csv".format(county), 'r')
  contents = fileIO.readlines()[const.NOAA_header_line_offset:]

  contents[0] = "Year,Month,Temperature\n"

  for i in range(1, len(contents)):
    fields = contents[i].split(',')

    record_year = fields[0][:4]
    record_month = fields[0][4:6]
    record_temp = fields[1]

    contents[i] = ','.join([record_year, record_month, record_temp])

  fileIO = open("./data/Temperature_By_County/{}_Temp.csv".format(county), 'w')
  fileIO.write(''.join(contents))
  fileIO.close()