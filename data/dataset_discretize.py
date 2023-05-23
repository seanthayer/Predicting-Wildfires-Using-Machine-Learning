# # #                # # #
#                        #
# Dataset discretization #
#                        #
# # #                # # #

# # #                                # # #
#                                        #
# This file is tagged: [WORK IN PROGESS] #
#                                        #
# # #                                # # #

import sys
import os
sys.path.append(os.getcwd())

import const
import multiprocessing as mp
import numpy as np
import util

from datetime import date, datetime
from json import dump

# # #

def aggregate_MonthlySums(array, month_indices):
  sums = []

  for i in range(0, len(month_indices) - 1):
    # Slice array at given month indices, filter null entries, and summate
    sums.append(sum(list(filter(None, array[month_indices[i]:month_indices[i + 1]]))))

  return sums

def aggregate_MonthlyMeans(array, month_indices):
  means = []

  for i in range(0, len(month_indices) - 1):
    # Slice array at given month indices, filter null entries, summate and average
    means.append( sum(list(filter(None, array[month_indices[i]:month_indices[i + 1]]))) / len(list(filter(None, array[month_indices[i]:month_indices[i + 1]]))) )

  return means

def partitionByYear(array_date):
  # There are other approaches that work here, but for simplicity we just iterate
  # ISO8601 date format

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
  # Just quickly iterate so we can disregard leap years and varying month lengths
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

# # #

def main():

  # Decimal latitude and longitude approximately equal to the grid's unit distance
  # Longitude takes the average of the unit distances at Oregon's northmost and southmost latitude for a decent fit
  unit_to_decimal_lat_approx = util.returnUnitDistanceLat(const.grid_unit_size, const.earth_radius)
  unit_to_decimal_lng_approx = (util.returnUnitDistanceLng(const.grid_unit_size, const.earth_radius, const.oregon_northmost_lat) + util.returnUnitDistanceLng(1, const.earth_radius, const.oregon_southmost_lat)) / 2

  # The number of rows and columns in the unit grid is the number of unit latitudes/longitudes it takes
  # to span Oregon from south to north and west to east
  unit_grid_rows = int(np.ceil((const.oregon_northmost_lat - const.oregon_southmost_lat) / unit_to_decimal_lat_approx))
  unit_grid_columns = int(np.ceil((const.oregon_eastmost_lng - const.oregon_westmost_lng) / unit_to_decimal_lng_approx))

  unit_grid_center_coords = []

  unit_grid = np.full((unit_grid_rows, unit_grid_columns), const.unit_grid_data_entry_empty, dtype = object)

  # Note the template spots, parameters, and units
  OPEN_METEO_query_base = "https://archive-api.open-meteo.com/v1/archive?latitude={}&longitude={}&start_date={}-01-01&end_date={}-12-31&daily=rain_sum,temperature_2m_max,temperature_2m_mean,temperature_2m_min,windspeed_10m_max,windgusts_10m_max,et0_fao_evapotranspiration&timezone=America%2FLos_Angeles&precipitation_unit=inch&temperature_unit=fahrenheit&windspeed_unit=mph"

  # Query the same range of time as the NOAA dataset
  query_year_start = const.NOAA_dataset_query_year_start
  query_year_stop = const.NOAA_dataset_query_year_stop

  # Number of chunks to break the total request into
  # This is especially important for high-resolution grids and low-memory machines
  query_chunks = 10
  query_chunk_size = int(np.ceil((unit_grid_rows * unit_grid_columns) / query_chunks))

  # # #

  print("---------------------------------BEGIN-----------------------------------")
  print("[INFO] Generated blank unit grid with the following attributes:")
  print("[INFO]   - Grid size:     {} x {}".format(unit_grid_rows, unit_grid_columns))
  print("[INFO]   - Cell size:")
  print("[INFO]     - ({} x {}):   {} x {}".format(const.info_unit_distance, const.info_unit_distance, const.grid_unit_size, const.grid_unit_size))
  print("[INFO]     - (lat x lng): {} x {}".format(unit_to_decimal_lat_approx, unit_to_decimal_lng_approx))
  print("[INFO]   - Total entries: {}".format(unit_grid_rows * unit_grid_columns))
  print("-------------------------------------------------------------------------")

  # Calculate and store the center point for each grid entry. Could be done on the fly, but this is simple
  for i in range(0, unit_grid_rows):
    for j in range(0, unit_grid_columns):
      center_lat = const.oregon_northmost_lat - ((unit_to_decimal_lat_approx / 2) + (unit_to_decimal_lat_approx * i))
      center_lng = const.oregon_westmost_lng + ((unit_to_decimal_lng_approx / 2) + (unit_to_decimal_lng_approx * j))

      unit_grid_center_coords.append((center_lat, center_lng))

  for chunk in range(0, query_chunks):
    print("[INFO] Processing chunk ({} / {})".format(chunk + 1, query_chunks))

    OPEN_METEO_datasets = []

    chunk_begin = chunk * query_chunk_size
    chunk_end = min((chunk + 1) * query_chunk_size, len(unit_grid_center_coords))

    print("[INFO] Begin OpenMeteo network requests:")
    print("[INFO]   - Thread team size: {}".format(mp.cpu_count()))
    print("[INFO]   - Chunk size:       {}".format(query_chunk_size))
    print("[INFO]   - Chunk range:      {} - {}".format(chunk_begin, chunk_end))
    print()

    # Create a thread team within a context manager, and generate a request task for
    # each center coord in the current chunk. If a request fails, leave that entry null and continue
    t_0 = datetime.now()
    with mp.Pool(mp.cpu_count()) as p:
      tasks = []
      for coord in unit_grid_center_coords[chunk_begin:chunk_end]:
        tasks.append(p.apply_async(util.requestGET, [OPEN_METEO_query_base.format(coord[0], coord[1], query_year_start, query_year_stop)]))
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

    # Currently, this snippet of code is mostly for show. The future intent is to allow recovery from
    # a certain chunk without having to repeat requests. Especially important for high-resolution grids
    fileIO = open("./data/datasets/__OPEN_METEO_CHUNK_{}.csv".format(chunk + 1), 'w')
    for dataset in OPEN_METEO_datasets:
      dump(dataset, fileIO)
    fileIO.close()

    print("[INFO] Begin dataset processing and unit grid assignment:")
    print("[INFO]   - Datasets: {}".format(len(OPEN_METEO_datasets)))
    print()

    # First, move along the current chunk of datasets, mapping to the proper row and column
    t_0 = datetime.now()
    for i in range(0, (chunk_end - chunk_begin)):
        if OPEN_METEO_datasets[i]:

          unit_grid_chunk_i = int(np.floor((chunk_begin + i) / unit_grid_columns))
          unit_grid_chunk_j = (chunk_begin + i) % unit_grid_columns

          unit_grid[unit_grid_chunk_i][unit_grid_chunk_j] = ''

          # OpenMeteo datasets return with a date array whose entries correspond to entries in the climate data
          year_partition_indices = partitionByYear(OPEN_METEO_datasets[i]["daily"]["time"])

          # Second, consider each year and process
          for j in range(0, len(year_partition_indices) - 1):

            # Find indices that partition the current year into months
            month_partition_indices = partitionByMonth(OPEN_METEO_datasets[i]["daily"]["time"][year_partition_indices[j]:year_partition_indices[j + 1]])
            
            # Aggregate daily data into monthly sums and means. Future versions may forego this aggregation in the interest of experimentation
            months_precips =          aggregate_MonthlySums(OPEN_METEO_datasets[i]["daily"]["rain_sum"][year_partition_indices[j]:year_partition_indices[j + 1]], month_partition_indices)
            months_temp_mean_maxima = aggregate_MonthlyMeans(OPEN_METEO_datasets[i]["daily"]["temperature_2m_max"][year_partition_indices[j]:year_partition_indices[j + 1]], month_partition_indices)
            months_temp_means =       aggregate_MonthlyMeans(OPEN_METEO_datasets[i]["daily"]["temperature_2m_mean"][year_partition_indices[j]:year_partition_indices[j + 1]], month_partition_indices)
            months_temp_mean_minima = aggregate_MonthlyMeans(OPEN_METEO_datasets[i]["daily"]["temperature_2m_min"][year_partition_indices[j]:year_partition_indices[j + 1]], month_partition_indices)
            
            months_wind_speed_mean_maxima = aggregate_MonthlyMeans(OPEN_METEO_datasets[i]["daily"]["windspeed_10m_max"][year_partition_indices[j]:year_partition_indices[j + 1]], month_partition_indices)
            months_wind_gusts_mean_maxima = aggregate_MonthlyMeans(OPEN_METEO_datasets[i]["daily"]["windgusts_10m_max"][year_partition_indices[j]:year_partition_indices[j + 1]], month_partition_indices)

            months_evapotranspiration_means = aggregate_MonthlyMeans(OPEN_METEO_datasets[i]["daily"]["et0_fao_evapotranspiration"][year_partition_indices[j]:year_partition_indices[j + 1]], month_partition_indices)

            # Third, monthly data belongs to its own row. See "unit_grid_data_header" in "const.py" for column schema (GridX and GridY are added at final parsing)
            for k in range(0, 12):

              unit_grid[unit_grid_chunk_i][unit_grid_chunk_j] += "{},{},{},{},,{},{},,,,,{},{},{},{},{},{},{},{}\n".format(
                
                unit_grid_center_coords[chunk_begin + i][1],
                unit_grid_center_coords[chunk_begin + i][0],
                OPEN_METEO_datasets[i]["longitude"],
                OPEN_METEO_datasets[i]["latitude"],
                date.fromisoformat(OPEN_METEO_datasets[i]["daily"]["time"][year_partition_indices[j]]).year,
                str(k + 1),
                months_precips[k],
                months_temp_mean_maxima[k],
                months_temp_means[k],
                months_temp_mean_minima[k],
                months_wind_speed_mean_maxima[k],
                months_wind_gusts_mean_maxima[k],
                months_evapotranspiration_means[k],
                "0"

              )

    t_1 = datetime.now()
    dt = (t_1 - t_0)

    print("[INFO] Complete unit grid population:")
    print("[INFO]   - Time elapsed: {}".format(dt))
    print("-------------------------------------------------------------------------")

  unit_grid_data = util.parseUnitGrid(unit_grid)

  fileIO = open("./data/datasets/Oregon_Unit_Grid_Base__{}{}__{}x{}.csv".format(const.grid_unit_size, const.info_unit_distance, unit_grid_rows, unit_grid_columns), 'w')
  fileIO.write(const.unit_grid_data_header)
  fileIO.writelines(unit_grid_data)
  fileIO.close()

  for chunk in range(query_chunks):
    os.remove("./data/datasets/_OPEN_METEO_CHUNK_{}.csv".format(chunk + 1))

  print("[INFO] Complete dataset discretization!")
  print("----------------------------------END-------------------------------------")

  return 0

# # #

if __name__ == "__main__":
  sys.exit(main())