# # #                     # # #
#                             #
# Automatic dataset retrieval #
#                             #
# # #                     # # #

# # #                             # # #
#                                     #
# This file is pending documentation! #
#                                     #
# # #                             # # #

import sys
import os
sys.path.append(os.getcwd())

import requests
import const as const

# # #

def matchState(entry, q_state_code):
  identifier_state_code = entry[:2]
  return (q_state_code == identifier_state_code)

def matchYear(entry, q_year_start, q_year_end):
  identifier_year = entry[7:11]

  for q_year in range(q_year_start, q_year_end + 1):
    if str(q_year) == identifier_year:
      return True

  return False

def quickPartition(stream, span_start):
  span_end = span_start
  c = stream[span_start]

  while c != '\n':
    span_end += 1
    c = stream[span_end]

  return (stream[span_start:span_end + 1], span_end + 1)

def iterEntriesByOffset(stream, record_length, span_start):
  if (span_start + record_length) >= len(stream):
    # Out of index exception
    return (None, None)
  else:
    return (stream[span_start:(span_start + record_length)], span_start + record_length)
  
def parseEntry(entry):
  identifier_county = entry[2:5]
  identifier_year = entry[7:11]

  county_name = const.county_list[const.NOAA_county_code_list.index(identifier_county)]
  data_monthly = entry[14:].split()
  
  return [",".join([county_name, identifier_year, str(i + 1), data_monthly[i]]) + '\n' for i in range(0, len(data_monthly))]

# # #                 # # #
#                         #
# NOAA Dataset Processing #
#                         #
# # #                 # # #

NOAA_datasets_URL = "https://www.ncei.noaa.gov/pub/data/cirs/climdiv/"

NOAA_state_code_oregon     = "35"
NOAA_element_code_precip   = "01"
NOAA_element_code_temp     = "02"
NOAA_element_code_max_temp = "27"
NOAA_element_code_min_temp = "28"

NOAA_filename_general_form = "climdiv-******-vx.y.z-YYYYMMDD"

NOAA_filename_prefix_precip   = "climdiv-pcpncy-"
NOAA_filename_prefix_temp     = "climdiv-tmpccy-"
NOAA_filename_prefix_max_temp = "climdiv-tmaxcy-"
NOAA_filename_prefix_min_temp = "climdiv-tmincy-"

NOAA_record_fixed_length = 99

NOAA_datasets = []

query_year_start = 2000
query_year_stop = 2022

query_data_type = ["Precipitation", "Temperature_Mean", "Temperature_Max", "Temperature_Min"] # Sorted in the order of dataset processing

# # #

print("[INFO] Requesting NOAA directory ({})".format(NOAA_datasets_URL))

request = requests.get(url = NOAA_datasets_URL)
NOAA_datasets_directory = request.text

NOAA_filename_precip_index = NOAA_datasets_directory.find(NOAA_filename_prefix_precip)
NOAA_filename_temp_index = NOAA_datasets_directory.find(NOAA_filename_prefix_temp)
NOAA_filename_max_temp_index = NOAA_datasets_directory.find(NOAA_filename_prefix_max_temp)
NOAA_filename_min_temp_index = NOAA_datasets_directory.find(NOAA_filename_prefix_min_temp)

NOAA_filename_precip = NOAA_datasets_directory[NOAA_filename_precip_index:NOAA_filename_precip_index + len(NOAA_filename_general_form)]
NOAA_filename_temp = NOAA_datasets_directory[NOAA_filename_temp_index:NOAA_filename_temp_index + len(NOAA_filename_general_form)]
NOAA_filename_max_temp = NOAA_datasets_directory[NOAA_filename_max_temp_index:NOAA_filename_max_temp_index + len(NOAA_filename_general_form)]
NOAA_filename_min_temp = NOAA_datasets_directory[NOAA_filename_min_temp_index:NOAA_filename_min_temp_index + len(NOAA_filename_general_form)]

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

# DEBUG = open("climdiv-pcpncy-v1.0.0-20230306.txt", 'r').read()
# NOAA_datasets.append(DEBUG)

print("[INFO] Dataset processing begin (n = {})".format(len(NOAA_datasets)))

for i in range(0, len(NOAA_datasets)):

  print("[INFO] Processing {} data (rows = {})".format(query_data_type[i], len(NOAA_datasets[i])))

  dataset_filtered = []

  entry, span_start = iterEntriesByOffset(NOAA_datasets[i], NOAA_record_fixed_length, 0)
  while not matchState(entry, NOAA_state_code_oregon):
    entry, span_start = iterEntriesByOffset(NOAA_datasets[i], NOAA_record_fixed_length, span_start)

  while entry:
    if matchState(entry, NOAA_state_code_oregon):
      if matchYear(entry, query_year_start, query_year_stop):
        dataset_filtered.extend(parseEntry(entry))
    elif len(dataset_filtered) > 0:
      break

    entry, span_start = iterEntriesByOffset(NOAA_datasets[i], NOAA_record_fixed_length, span_start)

  print("[INFO] Writing {} data (rows = {})".format(query_data_type[i], len(dataset_filtered)))

  # NOTE: This procedure currently writes data in a format non-conformant with other modules
  fileIO = open("./data/datasets/Oregon_{}.csv".format(query_data_type[i]), 'w')
  fileIO.write("County,Year,Month,{}\n".format(query_data_type[i]))
  fileIO.writelines(dataset_filtered)
  fileIO.close()

print("[INFO] Dataset processing complete!")

# # #
