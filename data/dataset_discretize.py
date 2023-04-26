# # #                # # #
#                        #
# Dataset discretization #
#                        #
# # #                # # #

# # #                                                         # # #
#                                                                 #
# This file is tagged: [WORK IN PROGESS], [PENDING DOCUMENTATION] #
#                                                                 #
# # #                                                         # # #

import sys
import os
sys.path.append(os.getcwd())

from datetime import date, datetime
import requests
import numpy as np
import const as const

# # #

def returnUnitDistanceLng(units_k, earth_radius, lat_deg):

  # Please see the "Haversine Formula" (https://en.wikipedia.org/wiki/Haversine_formula) for more insight into this function's foundation
  #
  # This function makes use of the Haversine Formula, which "determines the great-circle distance between two points on a sphere given their longitudes and latitudes."
  # However, this function derives an algebraically equivalent expression from the formula and instead determines a unit of longitude, corresponding to some unit(s) of distance,
  # given a latitude and measure of distance.

  c = units_k / earth_radius

  lat_radians = np.deg2rad(lat_deg)

  lng_unit_distance_radians = 2 * np.arcsin(np.sqrt( (np.sin(c / 2) ** 2) / (np.cos(lat_radians) ** 2) ))

  return np.rad2deg(lng_unit_distance_radians)

def returnUnitDistanceLat(units_k, earth_radius):
  
  # Variant of "returnUnitDistanceLng()"

  c = units_k / earth_radius

  lat_unit_distance_radians = 2 * np.arcsin(np.sqrt( (np.sin(c / 2) ** 2)))

  return np.rad2deg(lat_unit_distance_radians)

def parseRow(row):
  data = row.split(',')

  return {
    "county": data[0],
    "year": data[1],
    "month": data[2],
    "foundLng": data[3],
    "foundLat": data[4],
    "originLng": data[5],
    "originLat": data[6],
    "precipitation": data[7],
    "temperature": data[8],
    "temperature_max": data[9],
    "temperature_min": data[10].replace('\n', '') # Remove errant '\n'
  }

def partitionByYear(array_date):
  partitions_i = [0]

  oldYear = date.fromisoformat(array_date[0]).year

  for i in range(1, len(array_date)):
    newYear = date.fromisoformat(array_date[i]).year

    if oldYear != newYear:
      partitions_i.append(i)
      oldYear = newYear

  partitions_i.append(len(array_date) - 1)

  return partitions_i

def partitionByMonth(array_date):
  # Just quickly iterate so we don't have to worry about leap years
  # ISO8601 date format

  partitions_i = [0]

  oldMonth = date.fromisoformat(array_date[0]).month

  for i in range(1, len(array_date)):
    newMonth = date.fromisoformat(array_date[i]).month

    if oldMonth != newMonth:
      partitions_i.append(i)
      oldMonth = newMonth

  partitions_i.append(len(array_date) - 1)

  return partitions_i

def aggregateMonthlyPrecip(array_precip, month_indices):
  precips = []

  for i in range(0, len(month_indices) - 1):
    precips.append(sum(array_precip[month_indices[i]:month_indices[i + 1]]))

  return precips

def aggregateMonthlyTempMax(array_temp_max, month_indices):
  temp_maxes = []

  for i in range(0, len(month_indices) - 1):
    temp_maxes.append(max(array_temp_max[month_indices[i]:month_indices[i + 1]]))

  return temp_maxes

def aggregateMonthlyTempMin(array_temp_min, month_indices):
  temp_mins = []

  for i in range(0, len(month_indices) - 1):
    temp_mins.append(min(array_temp_min[month_indices[i]:month_indices[i + 1]]))

  return temp_mins

def aggregateMonthlyTempMean(array_temp_max, array_temp_min, month_indices):
  temp_means = []

  for i in range(0, len(month_indices) - 1):
    temp_means.append(( (sum(array_temp_max[month_indices[i]:month_indices[i + 1]]) / len(array_temp_max[month_indices[i]:month_indices[i + 1]])) + (sum(array_temp_min[month_indices[i]:month_indices[i + 1]]) / len(array_temp_min[month_indices[i]:month_indices[i + 1]])) ) / 2)

  return temp_means

def parseUnitGrid(unit_grid):
  unit_grid_data = []

  for i in range(0, unit_grid.shape[0]):
    row_parse = []
    for j in range(0, unit_grid.shape[1]):
      row_parse.extend((["{},{},".format(j, i) + row for row in unit_grid[i][j].splitlines(keepends = True)]) if unit_grid[i][j] else ["{},{},,,,,,,,,,,\n".format(j, i)])
    
    unit_grid_data.extend(row_parse)

  return unit_grid_data

# # #

earth_radius = 3958.75587 # in miles, swappable with other units of measure (e.g., km)

oregon_northmost_lat = 46.3
oregon_southmost_lat = 42.0
oregon_eastmost_lng = -116.46666667
oregon_westmost_lng = -124.63333333

# Let "unit" = 1 mile
unit_to_decimal_lat_approx = returnUnitDistanceLat(5, earth_radius)
unit_to_decimal_lng_approx = (returnUnitDistanceLng(5, earth_radius, oregon_northmost_lat) + returnUnitDistanceLng(1, earth_radius, oregon_southmost_lat)) / 2

unit_grid_rows = int(np.ceil((oregon_northmost_lat - oregon_southmost_lat) / unit_to_decimal_lat_approx))
unit_grid_columns = int(np.ceil((oregon_eastmost_lng - oregon_westmost_lng) / unit_to_decimal_lng_approx))

unit_grid = np.ndarray((unit_grid_rows, unit_grid_columns), dtype = object)
unit_grid.fill('')

OPEN_METEO_query_base = "https://archive-api.open-meteo.com/v1/archive?latitude={}&longitude={}&start_date={}-01-01&end_date={}-12-31&daily=rain_sum,apparent_temperature_max,apparent_temperature_min&timezone=America%2FLos_Angeles&precipitation_unit=inch&temperature_unit=fahrenheit"

query_year_start = const.NOAA_dataset_query_year_start
query_year_stop = const.NOAA_dataset_query_year_stop

# # #

# # #                                                                                                                                       # # #
#                                                                                                                                               #
# NOTE: This is an ad-hoc solution (apparent from the mess) for an attempted proof of concept. It aims to populate every entry within the unit  #
#       grid with values for precipitation, mean temperature, max temperature, and min temperature by querying the Open-Meteo weather           #
#       API with the latitudinal and longitudinal center of each grid unit.                                                                     #
#                                                                                                                                               #
#       In theory, it should complete and correctly populate the unit grid given enough time. However, network latency dominates execution time #
#       and makes the solution wholly intractable. For a rough estimate, a unit grid with a resolution of 1 mi^2 has 298 rows and 405 columns,  #
#       a matrix of size 298 x 405 = 120,690 entries. A full request and response over the network takes approx. 1.07 seconds to complete.      #
#       With a network request required for every entry, estimated execution time is                                                            #
#       (120,690 * 1.07) = 129,138.3 seconds ~= 2,152 minutes ~= 36 hours.                                                                      #
#                                                                                                                                               #
#       Possible solutions are listed below:                                                                                                    #
#                                                                                                                                               #
#         1. Decrease grid resolution                                                                                                           #
#                                                                                                                                               #
#             A decrease in grid resolution would significantly decrease the total number of entries that require a network request to          #
#             populate. However, even a grid with a resolution of 5 mi^2 will have 60 rows and 132 columns, a matrix with 7,920 entries.        #
#             Using the network latency from before: ~2 hours estimated execution time. Additionally, a decrease in resolution will likely      #
#             lead to decreased prediction power for a model trained on it.                                                                     #
#                                                                                                                                               #
#         2. Parallel processing                                                                                                                #
#                                                                                                                                               #
#             This solution is less certain. But assuming a CPU with 16 cores and hyperthreading is used, and network requests can be           #
#             opened concurrently for each thread, entries could hypothetically be processed in batches of 32 without incurring additional      #
#             network delay. This would reduce the total latency from network requests for a 1 mi^2 unit grid from                              #
#             (120,690 * 1.07) = 129,138.3 seconds ~= 36 hours to ((120,690 / 32) * 1.07) = 4,035.5 seconds ~= 1 hour. This is a major          #
#             improvement, but is contingent upon the previous assumptions and a 1:1 speedup for increasing thread counts (which it certainly   #
#             won't be).                                                                                                                        #
#                                                                                                                                               #
#         3. Implement a point location algorithm (see, the point location problem: https://en.wikipedia.org/wiki/Point_location)               #
#                                                                                                                                               #
#             The reason this ad-hoc solution was initially implemented, instead of using the already acquired NOAA dataset, is that there is   #
#             no common attribute to join on for units within the grid and rows within the NOAA dataset. The unit grid is defined in latitude   #
#             and longitude coordinate space, and the NOAA dataset is grouped by county. While a geospatial dataset (SHAPE file) was found in   #
#             Oregon's Spatial Data Library (https://spatialdata.oregonexplorer.info/geoportal/) which defines Oregon's counties as polygons    #
#             with vertices in latitude, longitude coordinate space, no packages were found that could easily take this data and perform simple #
#             point queries against it. If a point location algorithm was implemented, this could cut network latency times entirely out of the #
#             equation, almost certainly resulting in an overwhelming performance increase.                                                     #
#                                                                                                                                               #
# # #                                                                                                                                       # # #

for i in range(0, unit_grid.shape[0]):
  for j in range(0, unit_grid.shape[1]):
    query_lat = oregon_northmost_lat + (unit_to_decimal_lat_approx / 2) + (unit_to_decimal_lat_approx * i)
    query_lng = oregon_westmost_lng + (unit_to_decimal_lng_approx / 2) + (unit_to_decimal_lng_approx * i)

    request = requests.get(url = OPEN_METEO_query_base.format(query_lat, query_lng, query_year_start, query_year_stop))
    if request.status_code == 200:
      OPEN_METEO_dataset = request.json()

      year_partitions_i = partitionByYear(OPEN_METEO_dataset["daily"]["time"])

      for k in range(0, len(year_partitions_i) - 1):
        month_partitions_i = partitionByMonth(OPEN_METEO_dataset["daily"]["time"][year_partitions_i[k]:year_partitions_i[k + 1]])
        
        months_precip = aggregateMonthlyPrecip(OPEN_METEO_dataset["daily"]["rain_sum"][year_partitions_i[k]:year_partitions_i[k + 1]], month_partitions_i)
        months_temp_mean = aggregateMonthlyTempMean(OPEN_METEO_dataset["daily"]["apparent_temperature_max"][year_partitions_i[k]:year_partitions_i[k + 1]], OPEN_METEO_dataset["daily"]["apparent_temperature_min"][year_partitions_i[k]:year_partitions_i[k + 1]], month_partitions_i)
        months_temp_max = aggregateMonthlyTempMax(OPEN_METEO_dataset["daily"]["apparent_temperature_max"][year_partitions_i[k]:year_partitions_i[k + 1]], month_partitions_i)
        months_temp_min = aggregateMonthlyTempMin(OPEN_METEO_dataset["daily"]["apparent_temperature_min"][year_partitions_i[k]:year_partitions_i[k + 1]], month_partitions_i)
        
        for l in range(0, 12):
          unit_grid[i][j] += ",{},{},,,,,{},{},{},{}\n".format(date.fromisoformat(OPEN_METEO_dataset["daily"]["time"][year_partitions_i[k]]).year, l + 1, months_precip[l], months_temp_max[l], months_temp_max[l], months_temp_min[l])

      print("[DEV] Processed a grid unit ({} / {}), ({} / {}) ...".format(i, unit_grid_rows, j, unit_grid_columns))

    else:
      print("[ERROR] Open-Meteo returned a status code of: {}".format(request.status_code))

# # #

fileIO_incident_matrix = open("./data/datasets/Oregon_Incident_Matrix.csv", 'r')

incident_matrix = fileIO_incident_matrix.readlines()

fileIO_incident_matrix.close()

for i in range(1, len(incident_matrix)):
  incident = parseRow(incident_matrix[i])

  incident_unit_grid_row = int(np.ceil((oregon_northmost_lat - float(incident["foundLat"])) / unit_to_decimal_lat_approx))
  incident_unit_grid_column = int(np.ceil((float(incident["foundLng"]) - oregon_westmost_lng) / unit_to_decimal_lng_approx))

  unit_grid[incident_unit_grid_row - 1][incident_unit_grid_column - 1] += incident_matrix[i]

unit_grid_data = parseUnitGrid(unit_grid)

fileIO = open("./data/datasets/Oregon_Incident_Unit_Grid.csv", 'w')
fileIO.write("GridX,GridY,County,Year,Month,FoundLng,FoundLat,OriginLng,OriginLat,Precipitation,Temperature_Mean,Temperature_Max,Temperature_Min\n")
fileIO.writelines(unit_grid_data)
fileIO.close()