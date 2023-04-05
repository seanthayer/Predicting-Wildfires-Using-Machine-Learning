# # #                   # # #
#                           #
# Dataset joining procedure #
#                           #
# # #                   # # #

import sys
import os
sys.path.append(os.getcwd())

import const as const

# # #

def parseRow_NIFC(row):
  data = row.split(',')

  return { 
    "county": data[0],
    "year": data[1],
    "month": data[2],
    "day": data[3],
    "foundLng": data[4],
    "foundLat": data[5],
    "originLng": data[6],
    "originLat": data[7].replace('\n', '') # Remove errant '\n'
  }

def parseRow_NOAA(row):
  data = row.split(',')

  return {
    "county": data[0],
    "year": data[1],
    "month": data[2],
    "value": data[3].replace('\n', '') # Remove errant '\n'
  }

# # #

fileIO_NIFC_fire_incidence = open("./data/datasets/Oregon_Fire_Incidence.csv", 'r')
fileIO_NOAA_precip = open("./data/datasets/Oregon_Precipitation.csv", 'r')
fileIO_NOAA_temp = open("./data/datasets/Oregon_Temperature_Mean.csv", 'r')
fileIO_NOAA_max_temp = open("./data/datasets/Oregon_Temperature_Max.csv", 'r')
fileIO_NOAA_min_temp = open("./data/datasets/Oregon_Temperature_Min.csv", 'r')

NIFC_fire_incidence = fileIO_NIFC_fire_incidence.readlines()
NOAA_precip = fileIO_NOAA_precip.readlines()
NOAA_temp = fileIO_NOAA_temp.readlines()
NOAA_max_temp = fileIO_NOAA_max_temp.readlines()
NOAA_min_temp = fileIO_NOAA_min_temp.readlines()

fileIO_NOAA_min_temp.close()
fileIO_NOAA_max_temp.close()
fileIO_NOAA_temp.close()
fileIO_NOAA_precip.close()
fileIO_NIFC_fire_incidence.close()

# # #                                                                                                              # # #
#                                                                                                                      #
# NOTE: This procedure makes the following assumptions to operate correctly/efficiently,                               #
#                                                                                                                      #
#         1. All datasets share the attributes "County", "Year", and "Month"                                           #
#         2. All datasets are sorted in ascending order on the attributes "County", "Year", and "Month", consecutively #
#         3. The NIFC dataset may contain duplicates of the attributes "Year" and "Month"                              #
#         4. The NOAA datasets may NOT contain duplicate of the attributes "Year" and "Month"                          #
#                                                                                                                      #
# # #                                                                                                              # # #

join_matrix = []
isJoining = True

i = j = 1 # 1 to skip the header
while isJoining:
  row_NIFC = parseRow_NIFC(NIFC_fire_incidence[i])
  row_NOAA_precip = parseRow_NOAA(NOAA_precip[j])
  row_NOAA_temp = parseRow_NOAA(NOAA_temp[j])
  row_NOAA_max_temp = parseRow_NOAA(NOAA_max_temp[j])
  row_NOAA_min_temp = parseRow_NOAA(NOAA_min_temp[j])

  # NOAA datasets are uniform, so if there does/does not exist a join for one, it does/does not exist for all
  joinCounty = row_NIFC["county"] == row_NOAA_precip["county"]
  joinYear = row_NIFC["year"] == row_NOAA_precip["year"]
  joinMonth = row_NIFC["month"] == row_NOAA_precip["month"]

  if joinCounty and joinYear and joinMonth:
    join_matrix.append(','.join([
      row_NIFC["county"],
      row_NIFC["year"],
      row_NIFC["month"],
      row_NIFC["foundLng"],
      row_NIFC["foundLat"],
      row_NIFC["originLng"],
      row_NIFC["originLat"],
      row_NOAA_precip["value"],
      row_NOAA_temp["value"],
      row_NOAA_max_temp["value"],
      row_NOAA_min_temp["value"]
    ]) + '\n')

    i += 1
  elif joinCounty:
    j += 1
  elif const.county_list.index(row_NIFC["county"]) < const.county_list.index(row_NOAA_precip["county"]):
    i += 1
  elif const.county_list.index(row_NIFC["county"]) > const.county_list.index(row_NOAA_precip["county"]):
    j += 1

  if i >= len(NIFC_fire_incidence) or j >= len(NOAA_precip):
    isJoining = False

fileIO = open("./data/datasets/Oregon_Join_Matrix.csv", 'w')
fileIO.write("County,Year,Month,FoundLng,FoundLat,OriginLng,OriginLat,Precipitation,Temperature_Mean,Temperature_Max,Temperature_Min\n")
fileIO.writelines(join_matrix)
fileIO.close()