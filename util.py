# # #           # # #
#                   #
# Utility Functions #
#                   #
# # #           # # #

# # #                                      # # #
#                                              #
# This file is tagged: [PENDING DOCUMENTATION] #
#                                              #
# # #                                      # # #

import sys
import os
sys.path.append(os.getcwd())

import numpy as np
import requests

# # #

def parseUnitGrid(unit_grid):
  unit_grid_data = []

  for i in range(0, unit_grid.shape[0]):
    row_parse = []
    for j in range(0, unit_grid.shape[1]):
      row_parse.extend((["{},{},".format(j, i) + row for row in unit_grid[i][j].splitlines(keepends = True)]) if unit_grid[i][j] else ["{},{}\n".format(j, i)])
    
    unit_grid_data.extend(row_parse)

  return unit_grid_data

def parseUnitGridData(unit_grid_blank, unit_grid_data):
  for i in range(1, len(unit_grid_data)):
    row_parse = unit_grid_data[i].split(',')
    unit_grid_blank[int(row_parse[1])][int(row_parse[0])] += ','.join(row_parse[2:])

  return unit_grid_blank

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

def requestGET(url):
  req = requests.get(url)
  if req.status_code == 200:
    return req.json()
  else:
    print("[ERROR] {} returned code {}".format(url, req.status_code))
    return req.status_code