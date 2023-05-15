# # #                                                         # # #
#                                                                 #
# This file is tagged: [WORK IN PROGESS], [PENDING DOCUMENTATION] #
#                                                                 #
# # #                                                         # # #

import sys
import os
sys.path.append(os.getcwd())

import const
import numpy as np
import util

from datetime import datetime

# # #

def isJoin(row, attr_indices, query_attrs):
  row_parse = row.split(',')
  for i in range(0, len(attr_indices)):
    if row_parse[attr_indices[i]] != query_attrs[i]:
      return False

  return True

def parseRow_Incident(row):
  data = row.split(',')

  return {
    "county": data[0],
    "year": data[1],
    "month": data[2],
    "fireFoundLng": data[3],
    "fireFoundLat": data[4],
    "fireOriginLng": data[5],
    "fireOriginLat": data[6],
    "precipitation": data[7],
    "temperature": data[8],
    "temperature_max": data[9],
    "temperature_min": data[10].replace('\n', '') # Remove errant '\n'
  }

def spliceRow(row, attr_indices, attrs):
  row_parse = row.split(',')

  for i in range(0, len(attr_indices)):
    row_parse[attr_indices[i]] = attrs[i]

  return ','.join(row_parse)

# # #

def main():

  unit_to_decimal_lat_approx = util.returnUnitDistanceLat(const.grid_unit_size, const.earth_radius)
  unit_to_decimal_lng_approx = (util.returnUnitDistanceLng(const.grid_unit_size, const.earth_radius, const.oregon_northmost_lat) + util.returnUnitDistanceLng(1, const.earth_radius, const.oregon_southmost_lat)) / 2

  unit_grid_rows = int(np.ceil((const.oregon_northmost_lat - const.oregon_southmost_lat) / unit_to_decimal_lat_approx))
  unit_grid_columns = int(np.ceil((const.oregon_eastmost_lng - const.oregon_westmost_lng) / unit_to_decimal_lng_approx))

  unit_grid_blank = np.full((unit_grid_rows, unit_grid_columns), '', dtype = object)

  fileIO = open("./data/datasets/Oregon_Unit_Grid_Base__30mi__10x26.csv", 'r')
  unit_grid_data = fileIO.readlines()
  fileIO.close()

  unit_grid = util.parseUnitGridData(unit_grid_blank, unit_grid_data)

  fileIO = open("./data/datasets/Oregon_Incident_Matrix.csv", 'r')
  incident_matrix = fileIO.readlines()
  fileIO.close()

  print("[INFO] Begin fire incidence splicing:")
  print("[INFO]   - Incidents: {}".format(len(incident_matrix)))
  print()

  t_0 = datetime.now()
  for i in range(1, len(incident_matrix)):
    incident = parseRow_Incident(incident_matrix[i])

    incident_unit_grid_row = int(np.ceil((const.oregon_northmost_lat - float(incident["fireFoundLat"])) / unit_to_decimal_lat_approx))
    incident_unit_grid_column = int(np.ceil((float(incident["fireFoundLng"]) - const.oregon_westmost_lng) / unit_to_decimal_lng_approx))

    unit_cell_rows = unit_grid[incident_unit_grid_row - 1][incident_unit_grid_column - 1].splitlines(keepends = True)
    for j in range(0, len(unit_cell_rows)):
      if isJoin(unit_cell_rows[j], [5, 6], [incident["year"], incident["month"]]):
        row_splice = spliceRow(unit_cell_rows[j], [7, 8, 9, 10, 18], [incident["fireFoundLng"], incident["fireFoundLat"], incident["fireOriginLng"], incident["fireOriginLat"], "1\n"])
        unit_cell_rows.insert(j + 1, row_splice)

        if isJoin(unit_cell_rows[j], [18], ["0\n"]): del unit_cell_rows[j]
        break

    unit_grid[incident_unit_grid_row - 1][incident_unit_grid_column - 1] = ''.join(unit_cell_rows)

  t_1 = datetime.now()
  dt = (t_1 - t_0)

  print("[INFO] Complete fire incidence splicing:")
  print("[INFO]   - Time elapsed: {}".format(dt))

  # # #

  unit_grid_data = util.parseUnitGrid(unit_grid)

  fileIO = open("./data/datasets/Oregon_Unit_Grid_Train__{}{}__{}x{}.csv".format(const.grid_unit_size, const.info_unit_distance, unit_grid_rows, unit_grid_columns), 'w')
  fileIO.write(const.unit_grid_data_header)
  fileIO.writelines(unit_grid_data)
  fileIO.close()

  return 0

# # #

if __name__ == "__main__":
  sys.exit(main())