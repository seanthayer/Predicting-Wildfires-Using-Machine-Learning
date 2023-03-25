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

import datetime
import requests

# # #

def matchState(q_line, q_state_code):
  identifier_state_code = q_line[:2]
  return (q_state_code == identifier_state_code)

def matchYear(q_line, q_year_start, q_year_end):
  identifier_year = q_line[7:11]

  q_year_span = [str(q_year_start + i) for i in range(0, (q_year_end - q_year_start) + 1)]

  for q_year in q_year_span:
    if q_year == identifier_year:
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

# # #

NOAA_datasets_URL = "https://www.ncei.noaa.gov/pub/data/cirs/climdiv/"

NOAA_state_code_oregon     = "35"
NOAA_element_code_precip   = "01"
NOAA_element_code_temp     = "02"
NOAA_element_code_max_temp = "27"
NOAA_element_code_min_temp = "28"

NOAA_filename_general_form = "climdiv-******-vx.y.z-YYYYMMDD"

NOAA_filename_prefix_precip   = "climdiv-pcpncy-"
NOAA_filename_prefix_temp     = "climdiv-tmaxcy-"
NOAA_filename_prefix_max_temp = "climdiv-tmincy-"
NOAA_filename_prefix_min_temp = "climdiv-tmpccy-"

NOAA_record_fixed_length = 99

NOAA_datasets = []

query_year_start = 2000
query_year_stop = 2022

# # #

request = requests.get(url = NOAA_datasets_URL)
NOAA_datasets_directory = request.text

NOAA_filename_precip_index = NOAA_datasets_directory.find("climdiv-pcpncy-")
NOAA_filename_temp_index = NOAA_datasets_directory.find("climdiv-tmaxcy-")

NOAA_filename_precip = NOAA_datasets_directory[NOAA_filename_precip_index:NOAA_filename_precip_index + len(NOAA_filename_general_form)]
NOAA_filename_temp = NOAA_datasets_directory[NOAA_filename_temp_index:NOAA_filename_temp_index + len(NOAA_filename_general_form)]

request = requests.get(url = NOAA_datasets_URL + NOAA_filename_precip)
NOAA_datasets.append(request.text)

request = requests.get(url = NOAA_datasets_URL + NOAA_filename_temp)
NOAA_datasets.append(request.text)

for i in range(0, len(NOAA_datasets)):

  dataset_filtered = []

  entry, span_start = iterEntriesByOffset(NOAA_datasets[i], NOAA_record_fixed_length, 0)
  while not matchState(entry, NOAA_state_code_oregon):
    entry, span_start = iterEntriesByOffset(NOAA_datasets[i], NOAA_record_fixed_length, span_start)

  while entry:
    if matchState(entry, NOAA_state_code_oregon):
      if matchYear(entry, query_year_start, query_year_stop):
        dataset_filtered.append(entry)
    elif len(dataset_filtered) > 0:
      break

    entry, span_start = iterEntriesByOffset(NOAA_datasets[i], NOAA_record_fixed_length, span_start)

  NOAA_datasets[i] = dataset_filtered

# Write to .csv