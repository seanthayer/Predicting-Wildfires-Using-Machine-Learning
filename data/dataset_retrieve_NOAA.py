# # #                     # # #
#                             #
# Automatic dataset retrieval #
#                             #
# # #                     # # #

import sys
import os
sys.path.append(os.getcwd())

import const
import requests

# # #

# The procedures in this file expect each row of data received by NOAA to have this general form:
#
#   SSCCCTTYYYY 000.00 000.00 000.00 000.00 000.00 000.00 000.00 000.00 000.00 000.00 000.00 000.00
#   ^---------^ ^---------------------------------------------------------------------------------^
#        |                                                |
#   Identifier                        Data entries corresponding to each month
#
# Where,
#
# S = State code
# C = County code
# T = Datatype code (e.g., precipitation)
# Y = Year

# # #

def matchState(row, q_state_code):
  identifier_state_code = row[:2]
  return (q_state_code == identifier_state_code)

def matchYear(row, q_year_start, q_year_end):
  identifier_year = row[7:11]

  for q_year in range(q_year_start, q_year_end + 1):
    if str(q_year) == identifier_year:
      return True

  return False

def iterRowsByOffset(stream, record_length, span_start):
  if (span_start + record_length) >= len(stream):
    # Out of index exception
    return (None, None)
  else:
    # Return a tuple containing a single record of the stream and the offset of the next record
    return (stream[span_start:(span_start + record_length)], span_start + record_length)
  
def parseRow(row):
  identifier_county = row[2:5]
  identifier_year = row[7:11]

  # Select the county name from "county_list" corresponding to the county code in "NOAA_county_code_list"
  county_name = const.county_list[const.NOAA_county_code_list.index(identifier_county)]

  # Split data entries separated by the space character (' '), ignoring the row identifier
  data_monthly = row[11:].split()
  
  # Generate a new row for each month, joining data in standard .csv format: "[County],[Year],[Month (int)],[Data]\n"
  return [",".join([county_name, identifier_year, str(i + 1), data_monthly[i]]) + '\n' for i in range(0, len(data_monthly))]

# # #                 # # #
#                         #
# NOAA Dataset Processing #
#                         #
# # #                 # # #

def main():

  NOAA_datasets_URL = "https://www.ncei.noaa.gov/pub/data/cirs/climdiv/"

  NOAA_state_code_oregon     = "35"

  NOAA_filename_general_form = "climdiv-******-vx.y.z-YYYYMMDD"

  NOAA_filename_prefix_precip   = "climdiv-pcpncy-"
  NOAA_filename_prefix_temp     = "climdiv-tmpccy-"
  NOAA_filename_prefix_max_temp = "climdiv-tmaxcy-"
  NOAA_filename_prefix_min_temp = "climdiv-tmincy-"

  NOAA_record_fixed_length = 99

  NOAA_datasets = []

  query_year_start = const.NOAA_dataset_query_year_start
  query_year_stop = const.NOAA_dataset_query_year_stop

  query_data_type = ["Precipitation", "Temperature_Mean", "Temperature_Max", "Temperature_Min"] # Sorted in the order of dataset processing

  # # #

  print("[INFO] Requesting NOAA directory ({})".format(NOAA_datasets_URL))

  request = requests.get(url = NOAA_datasets_URL)
  NOAA_datasets_directory = request.text

  # Find the index positions in the response for each datatype's filename prefix
  NOAA_filename_precip_index = NOAA_datasets_directory.find(NOAA_filename_prefix_precip)
  NOAA_filename_temp_index = NOAA_datasets_directory.find(NOAA_filename_prefix_temp)
  NOAA_filename_max_temp_index = NOAA_datasets_directory.find(NOAA_filename_prefix_max_temp)
  NOAA_filename_min_temp_index = NOAA_datasets_directory.find(NOAA_filename_prefix_min_temp)

  # Select the actual filename for each datatype
  NOAA_filename_precip = NOAA_datasets_directory[NOAA_filename_precip_index:(NOAA_filename_precip_index + len(NOAA_filename_general_form))]
  NOAA_filename_temp = NOAA_datasets_directory[NOAA_filename_temp_index:(NOAA_filename_temp_index + len(NOAA_filename_general_form))]
  NOAA_filename_max_temp = NOAA_datasets_directory[NOAA_filename_max_temp_index:(NOAA_filename_max_temp_index + len(NOAA_filename_general_form))]
  NOAA_filename_min_temp = NOAA_datasets_directory[NOAA_filename_min_temp_index:(NOAA_filename_min_temp_index + len(NOAA_filename_general_form))]

  # Request each dataset in sequence and store in memory: Precipitation, Temperature (Mean), Temperature (Max), Temperature (Min)
  print("[INFO] Requesting {} data ({})".format(query_data_type[0], NOAA_datasets_URL + NOAA_filename_precip))

  request = requests.get(url = NOAA_datasets_URL + NOAA_filename_precip)
  NOAA_datasets.append(request.text)

  print("[INFO] Requesting {} data ({})".format(query_data_type[1], NOAA_datasets_URL + NOAA_filename_temp))

  request = requests.get(url = NOAA_datasets_URL + NOAA_filename_temp)
  NOAA_datasets.append(request.text)

  print("[INFO] Requesting {} data ({})".format(query_data_type[2], NOAA_datasets_URL + NOAA_filename_max_temp))

  request = requests.get(url = NOAA_datasets_URL + NOAA_filename_max_temp)
  NOAA_datasets.append(request.text)

  print("[INFO] Requesting {} data ({})".format(query_data_type[3], NOAA_datasets_URL + NOAA_filename_min_temp))

  request = requests.get(url = NOAA_datasets_URL + NOAA_filename_min_temp)
  NOAA_datasets.append(request.text)

  print("[INFO] Dataset processing begin (n = {})".format(len(NOAA_datasets)))

  for i in range(0, len(NOAA_datasets)):

    print("[INFO] Processing {} data (rows = {})".format(query_data_type[i], len(NOAA_datasets[i])))

    dataset_filtered = []

    row, span_start = iterRowsByOffset(NOAA_datasets[i], NOAA_record_fixed_length, 0)

    # Iterate over rows, parsing and storing those matching the state code and query year
    while row:
      if matchState(row, NOAA_state_code_oregon):
        if matchYear(row, query_year_start, query_year_stop):
          dataset_filtered.extend(parseRow(row))
      elif len(dataset_filtered) > 0:
        # Stop early when encountering a new state code to prevent unnecessary iteration
        break

      row, span_start = iterRowsByOffset(NOAA_datasets[i], NOAA_record_fixed_length, span_start)

    print("[INFO] Writing {} data (rows = {})".format(query_data_type[i], len(dataset_filtered)))

    fileIO = open("./data/datasets/Oregon_{}.csv".format(query_data_type[i]), 'w')
    fileIO.write(const.NOAA_data_header.format(query_data_type[i]))
    fileIO.writelines(dataset_filtered)
    fileIO.close()

  print("[INFO] Dataset processing complete!")

  return 0

# # #

if __name__ == "__main__":
  sys.exit(main())