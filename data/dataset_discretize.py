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

from json import dump
from datetime import date, datetime
import requests
import numpy as np
import multiprocessing as mp
import const as const

# # #

def aggregate_MonthlyPrecips(array_precip, month_indices):
  precips = []

  for i in range(0, len(month_indices) - 1):
    precips.append(sum(list(filter(None, array_precip[month_indices[i]:month_indices[i + 1]]))))

  return precips

def aggregate_MonthlyMaxTempMeans(array_temp_max, month_indices):
  temp_mean_maxima = []

  for i in range(0, len(month_indices) - 1):
    temp_mean_maxima.append( sum(list(filter(None, array_temp_max[month_indices[i]:month_indices[i + 1]]))) / len(list(filter(None, array_temp_max[month_indices[i]:month_indices[i + 1]]))) )

  return temp_mean_maxima

def aggregate_MonthlyTempMeans(array_temp_mean, month_indices):
  temp_means = []

  for i in range(0, len(month_indices) - 1):
    temp_means.append( sum(list(filter(None, array_temp_mean[month_indices[i]:month_indices[i + 1]]))) / len(list(filter(None, array_temp_mean[month_indices[i]:month_indices[i + 1]]))) )

  return temp_means

def aggregate_MonthlyMinTempMeans(array_temp_min, month_indices):
  temp_mean_minima = []

  for i in range(0, len(month_indices) - 1):
    temp_mean_minima.append( sum(list(filter(None, array_temp_min[month_indices[i]:month_indices[i + 1]]))) / len(list(filter(None, array_temp_min[month_indices[i]:month_indices[i + 1]]))) )

  return temp_mean_minima

def aggregate_MonthlyMaxWindSpeedMeans(array_wind_speed_max, month_indices):
  wind_speed_mean_maxima = []

  for i in range(0, len(month_indices) - 1):
    wind_speed_mean_maxima.append( sum(list(filter(None, array_wind_speed_max[month_indices[i]:month_indices[i + 1]]))) / len(list(filter(None, array_wind_speed_max[month_indices[i]:month_indices[i + 1]]))) )

  return wind_speed_mean_maxima

def aggregate_MonthlyMaxWindGustMeans(array_wind_gusts_max, month_indices):
  wind_gusts_mean_maxima = []

  for i in range(0, len(month_indices) - 1):
    wind_gusts_mean_maxima.append( sum(list(filter(None, array_wind_gusts_max[month_indices[i]:month_indices[i + 1]]))) / len(list(filter(None, array_wind_gusts_max[month_indices[i]:month_indices[i + 1]]))) )

  return wind_gusts_mean_maxima

def aggregate_MonthlyEvapotranspirationMeans(array_et, month_indices):
  et_means = []

  for i in range(0, len(month_indices) - 1):
    et_means.append( sum(list(filter(None, array_et[month_indices[i]:month_indices[i + 1]]))) / len(list(filter(None, array_et[month_indices[i]:month_indices[i + 1]]))) )

  return et_means

def isJoin(row, attr_indices, query_attrs):
  row_parse = row.split(',')
  for i in range(0, len(attr_indices)):
    if row_parse[attr_indices[i]] != query_attrs[i]:
      return False

  return True

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

def parseIncidentRow(row):
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

def parseUnitGrid(unit_grid):
  unit_grid_data = []

  for i in range(0, unit_grid.shape[0]):
    row_parse = []
    for j in range(0, unit_grid.shape[1]):
      row_parse.extend((["{},{},".format(j, i) + row for row in unit_grid[i][j].splitlines(keepends = True)]) if unit_grid[i][j] else ["{},{},,,,,,,,,,,,,,,,,,\n".format(j, i)])
    
    unit_grid_data.extend(row_parse)

  return unit_grid_data

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
  
def spliceRow(row, attr_indices, attrs):
  row_parse = row.split(',')

  for i in range(0, len(attr_indices)):
    row_parse[attr_indices[i]] = attrs[i]

  return ','.join(row_parse)

# # #

def main():

  earth_radius = 3958.75587 # In miles, swappable with other units of measure (e.g., km)
  info_unit_distance = "mi" # Only used for console logs

  oregon_northmost_lat = 46.3
  oregon_southmost_lat = 42.0
  oregon_eastmost_lng = -116.46666667
  oregon_westmost_lng = -124.63333333

  grid_unit_size = 35 # Units correspond to the measurement used for Earth's radius
  unit_to_decimal_lat_approx = returnUnitDistanceLat(grid_unit_size, earth_radius)
  unit_to_decimal_lng_approx = (returnUnitDistanceLng(grid_unit_size, earth_radius, oregon_northmost_lat) + returnUnitDistanceLng(1, earth_radius, oregon_southmost_lat)) / 2

  unit_grid_rows = int(np.ceil((oregon_northmost_lat - oregon_southmost_lat) / unit_to_decimal_lat_approx))
  unit_grid_columns = int(np.ceil((oregon_eastmost_lng - oregon_westmost_lng) / unit_to_decimal_lng_approx))

  unit_grid_center_coords = []

  unit_grid = np.ndarray((unit_grid_rows, unit_grid_columns), dtype = object)
  unit_grid.fill(",,,,,,,,,,,,,,,,,\n")

  OPEN_METEO_query_base = "https://archive-api.open-meteo.com/v1/archive?latitude={}&longitude={}&start_date={}-01-01&end_date={}-12-31&daily=rain_sum,temperature_2m_max,temperature_2m_mean,temperature_2m_min,windspeed_10m_max,windgusts_10m_max,et0_fao_evapotranspiration&timezone=America%2FLos_Angeles&precipitation_unit=inch&temperature_unit=fahrenheit&windspeed_unit=mph"

  query_year_start = const.NOAA_dataset_query_year_start
  query_year_stop = const.NOAA_dataset_query_year_stop

  query_chunks = 10
  query_chunk_size = int(np.ceil((unit_grid_rows * unit_grid_columns) / query_chunks))

  # # #

  print("---------------------------------BEGIN-----------------------------------")
  print("[INFO] Generated blank unit grid with the following attributes:")
  print("[INFO]   - Grid size:     {} x {}".format(unit_grid_rows, unit_grid_columns))
  print("[INFO]   - Cell size:")
  print("[INFO]     - ({} x {}):   {} x {}".format(info_unit_distance, info_unit_distance, grid_unit_size, grid_unit_size))
  print("[INFO]     - (lat x lng): {} x {}".format(unit_to_decimal_lat_approx, unit_to_decimal_lng_approx))
  print("[INFO]   - Total entries: {}".format(unit_grid_rows * unit_grid_columns))
  print("-------------------------------------------------------------------------")

  for i in range(0, unit_grid_rows):
    for j in range(0, unit_grid_columns):
      center_lat = oregon_northmost_lat - ((unit_to_decimal_lat_approx / 2) + (unit_to_decimal_lat_approx * i))
      center_lng = oregon_westmost_lng + ((unit_to_decimal_lng_approx / 2) + (unit_to_decimal_lng_approx * j))

      unit_grid_center_coords.append((center_lat, center_lng))

  for chunk in range(0, query_chunks):
    print("[INFO] Processing chunk ({} / {})".format(chunk + 1, query_chunks))

    OPEN_METEO_datasets = []

    chunk_begin = chunk * query_chunk_size
    chunk_end = chunk_begin + min(query_chunk_size, len(unit_grid_center_coords) - (chunk * query_chunk_size))

    print("[INFO] Begin OpenMeteo network requests:")
    print("[INFO]   - Thread team size: {}".format(mp.cpu_count()))
    print("[INFO]   - Chunk size:       {}".format(query_chunk_size))
    print("[INFO]   - Chunk range:      {} - {}".format(chunk_begin, chunk_end))
    print()

    t_0 = datetime.now()
    with mp.Pool(mp.cpu_count()) as p:
      tasks = []
      for coord in unit_grid_center_coords[chunk_begin:chunk_end]:
        tasks.append(p.apply_async(requestGET, [OPEN_METEO_query_base.format(coord[0], coord[1], query_year_start, query_year_stop)]))
      for t in tasks:
        result = t.get()

        if type(result) == dict:
          OPEN_METEO_datasets.append(result)
        else:
          OPEN_METEO_datasets.append(None)

    t_1 = datetime.now()
    dt = (t_1 - t_0)

    print("[INFO] Complete OpenMeteo network requests:")
    print("[INFO]   - Requests sent:        {}".format(query_chunk_size))
    print("[INFO]   - Responses successful: {}".format(len(list(filter(None, OPEN_METEO_datasets)))))
    print("[INFO]   - Time elapsed:         {}".format(dt))
    print()

    fileIO = open("./data/datasets/_OPEN_METEO_CHUNK_{}.csv".format(chunk + 1), 'w')
    for dataset in OPEN_METEO_datasets:
      dump(dataset, fileIO)
    fileIO.close()

    print("[INFO] Begin dataset processing and unit grid assignment:")
    print("[INFO]   - Datasets: {}".format(len(OPEN_METEO_datasets)))
    print()

    t_0 = datetime.now()
    for i in range(0, (chunk_end - chunk_begin)):
        if OPEN_METEO_datasets[i]:
          unit_grid[int(np.floor((chunk_begin + i) / unit_grid_columns))][(chunk_begin + i) % unit_grid_columns] = ''

          year_partitions_i = partitionByYear(OPEN_METEO_datasets[i]["daily"]["time"])

          for k in range(0, len(year_partitions_i) - 1):
            month_partitions_i = partitionByMonth(OPEN_METEO_datasets[i]["daily"]["time"][year_partitions_i[k]:year_partitions_i[k + 1]])
            
            months_precips =          aggregate_MonthlyPrecips(OPEN_METEO_datasets[i]["daily"]["rain_sum"][year_partitions_i[k]:year_partitions_i[k + 1]], month_partitions_i)
            months_temp_mean_maxima = aggregate_MonthlyMaxTempMeans(OPEN_METEO_datasets[i]["daily"]["temperature_2m_max"][year_partitions_i[k]:year_partitions_i[k + 1]], month_partitions_i)
            months_temp_means =       aggregate_MonthlyTempMeans(OPEN_METEO_datasets[i]["daily"]["temperature_2m_mean"][year_partitions_i[k]:year_partitions_i[k + 1]], month_partitions_i)
            months_temp_mean_minima = aggregate_MonthlyMinTempMeans(OPEN_METEO_datasets[i]["daily"]["temperature_2m_min"][year_partitions_i[k]:year_partitions_i[k + 1]], month_partitions_i)
            
            months_wind_speed_mean_maxima = aggregate_MonthlyMaxWindSpeedMeans(OPEN_METEO_datasets[i]["daily"]["windspeed_10m_max"][year_partitions_i[k]:year_partitions_i[k + 1]], month_partitions_i)
            months_wind_gusts_mean_maxima = aggregate_MonthlyMaxWindGustMeans(OPEN_METEO_datasets[i]["daily"]["windgusts_10m_max"][year_partitions_i[k]:year_partitions_i[k + 1]], month_partitions_i)

            months_evapotranspiration_means = aggregate_MonthlyEvapotranspirationMeans(OPEN_METEO_datasets[i]["daily"]["et0_fao_evapotranspiration"][year_partitions_i[k]:year_partitions_i[k + 1]], month_partitions_i)

            for l in range(0, 12):
              unit_grid[int(np.floor((chunk_begin + i) / unit_grid_columns))][(chunk_begin + i) % unit_grid_columns] += "{},{},{},{},,{},{},,,,,{},{},{},{},{},{},{}\n".format(
                
                unit_grid_center_coords[chunk_begin + i][1],
                unit_grid_center_coords[chunk_begin + i][0],
                OPEN_METEO_datasets[i]["longitude"],
                OPEN_METEO_datasets[i]["latitude"],
                date.fromisoformat(OPEN_METEO_datasets[i]["daily"]["time"][year_partitions_i[k]]).year,
                l + 1,
                months_precips[l],
                months_temp_mean_maxima[l],
                months_temp_means[l],
                months_temp_mean_minima[l],
                months_wind_speed_mean_maxima[l],
                months_wind_gusts_mean_maxima[l],
                months_evapotranspiration_means[l]

              )

    t_1 = datetime.now()
    dt = (t_1 - t_0)

    print("[INFO] Complete unit grid population:")
    print("[INFO]   - Time elapsed: {}".format(dt))
    print("-------------------------------------------------------------------------")

  unit_grid_data = parseUnitGrid(unit_grid)

  fileIO = open("./data/datasets/Oregon_Unit_Grid_Base.csv", 'w')
  fileIO.write("GridX,GridY,GridCenterLng,GridCenterLat,OpenMeteoLng,OpenMeteoLat,County,Year,Month,FireFoundLng,FireFoundLat,FireOriginLng,FireOriginLat,Precipitation,Temperature_Mean_Max,Temperature_Mean,Temperature_Mean_Min,Wind_Speed_Mean_Max,Wind_Gusts_Mean_Max,Evapotranspiration_Mean\n")
  fileIO.writelines(unit_grid_data)
  fileIO.close()

  for chunk in range(query_chunks):
    os.remove("./data/datasets/_OPEN_METEO_CHUNK_{}.csv".format(chunk + 1))

  # # #

  fileIO = open("./data/datasets/Oregon_Incident_Matrix.csv", 'r')
  incident_matrix = fileIO.readlines()
  fileIO.close()

  print("[INFO] Begin fire incidence splicing:")
  print("[INFO]   - Incidents: {}".format(len(incident_matrix)))
  print()

  t_0 = datetime.now()
  for i in range(1, len(incident_matrix)):
    incident = parseIncidentRow(incident_matrix[i])

    incident_unit_grid_row = int(np.ceil((oregon_northmost_lat - float(incident["fireFoundLat"])) / unit_to_decimal_lat_approx))
    incident_unit_grid_column = int(np.ceil((float(incident["fireFoundLng"]) - oregon_westmost_lng) / unit_to_decimal_lng_approx))

    unit_cell_rows = unit_grid[incident_unit_grid_row - 1][incident_unit_grid_column - 1].splitlines(keepends = True)
    for j in range(0, len(unit_cell_rows)):
      if isJoin(unit_cell_rows[j], [5, 6, 7, 8], [incident["year"], incident["month"], '', '']):
        row_splice = spliceRow(unit_cell_rows[j], [7, 8, 9, 10], [incident["fireFoundLng"], incident["fireFoundLat"], incident["fireOriginLng"], incident["fireOriginLat"]])
        unit_cell_rows.insert(j + 1, row_splice)

    unit_grid[incident_unit_grid_row - 1][incident_unit_grid_column - 1] = ''.join(unit_cell_rows)

  t_1 = datetime.now()
  dt = (t_1 - t_0)

  print("[INFO] Complete fire incidence splicing:")
  print("[INFO]   - Time elapsed: {}".format(dt))
  print()

  # # #

  unit_grid_data = parseUnitGrid(unit_grid)

  fileIO = open("./data/datasets/Oregon_Unit_Grid.csv", 'w')
  fileIO.write("GridX,GridY,GridCenterLng,GridCenterLat,OpenMeteoLng,OpenMeteoLat,County,Year,Month,FireFoundLng,FireFoundLat,FireOriginLng,FireOriginLat,Precipitation,Temperature_Mean_Max,Temperature_Mean,Temperature_Mean_Min,Wind_Speed_Mean_Max,Wind_Gusts_Mean_Max,Evapotranspiration_Mean\n")
  fileIO.writelines(unit_grid_data)
  fileIO.close()

  print("[INFO] Complete dataset discretization!")
  print("----------------------------------END-------------------------------------")

  return 0

# # #

if __name__ == "__main__":
  sys.exit(main())