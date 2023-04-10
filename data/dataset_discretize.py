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

  lng_unit_distance_radians = 2 * np.arcsin(np.sqrt( (np.sin(c / 2) ** 2) / np.cos(lat_radians) ** 2))

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

def parseUnitGrid(unit_grid):
  unit_grid_data = []

  for i in range(0, unit_grid.shape[0]):
    row_parse = []
    for j in range(0, unit_grid.shape[1]):
      row_parse.append("{},{},".format(j, i) + unit_grid[i][j] + '\n' if unit_grid[i][j] else "{},{},,,,,,,,,,,\n".format(j, i))
    
    unit_grid_data.extend(row_parse)

  return unit_grid_data

# # #

earth_radius = 3958.75587 # in miles, swappable with other units of measure (e.g., km)

oregon_northmost_lat = 46.3
oregon_southmost_lat = 42.0
oregon_eastmost_lng = -116.46666667
oregon_westmost_lng = -124.63333333

# Let "unit" = 1 mile
unit_to_decimal_lat_approx = returnUnitDistanceLat(1, earth_radius)
unit_to_decimal_lng_approx = (returnUnitDistanceLng(1, earth_radius, oregon_northmost_lat) + returnUnitDistanceLng(1, earth_radius, oregon_southmost_lat)) / 2

unit_grid_rows = int(np.ceil((oregon_northmost_lat - oregon_southmost_lat) / unit_to_decimal_lat_approx))
unit_grid_columns = int(np.ceil((oregon_eastmost_lng - oregon_westmost_lng) / unit_to_decimal_lng_approx))

unit_grid = np.ndarray((unit_grid_rows, unit_grid_columns), dtype = object)

# # #

fileIO_incident_matrix = open("./data/datasets/Oregon_Incident_Matrix.csv", 'r')

incident_matrix = fileIO_incident_matrix.readlines()

fileIO_incident_matrix.close()

for i in range(1, len(incident_matrix)):
  incident = parseRow(incident_matrix[i])

  incident_unit_grid_row = int(np.ceil((oregon_northmost_lat - float(incident["foundLat"])) / unit_to_decimal_lat_approx))
  incident_unit_grid_column = int(np.ceil((float(incident["foundLng"]) - oregon_westmost_lng) / unit_to_decimal_lng_approx))

  unit_grid[incident_unit_grid_row - 1][incident_unit_grid_column - 1] = incident_matrix[i]

unit_grid_data = parseUnitGrid(unit_grid)

fileIO = open("./data/datasets/Oregon_Incident_Unit_Grid.csv", 'w')
fileIO.write("GridX,GridY,County,Year,Month,FoundLng,FoundLat,OriginLng,OriginLat,Precipitation,Temperature_Mean,Temperature_Max,Temperature_Min\n")
fileIO.writelines(unit_grid_data)
fileIO.close()