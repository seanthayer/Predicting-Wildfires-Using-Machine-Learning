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

from datetime import date

# # #

def parseRow(row):
  county = row["attributes"]["POOCounty"]
  fireDate = date.fromtimestamp(row["attributes"]["FireDiscoveryDateTime"] // 1000) # Rounding division by 1000 to convert UNIX Epoch time from milliseconds to seconds
  fireFoundLng = row["geometry"]["x"]
  fireFoundLat = row["geometry"]["y"]
  fireOriginLng = row["attributes"]["InitialLongitude"] if row["attributes"]["InitialLongitude"] else ''
  fireOriginLat = row["attributes"]["InitialLatitude"] if row["attributes"]["InitialLatitude"] else ''

  return ",".join([county, str(fireDate.year), str(fireDate.month), str(fireDate.day), str(fireFoundLng), str(fireFoundLat), str(fireOriginLng), str(fireOriginLat)]) + '\n'

# # #                 # # #
#                         #
# NIFC Dataset Processing #
#                         #
# # #                 # # #

def main():
    
  # The query string in this URL contains various parameters relevant to dataset filtering (e.g., where=, outFields=, resultOffset=, etc.)
  NIFC_esri_feature_service_query_base = "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/WFIGS_Incident_Locations/FeatureServer/0/query?where=POOState%3D%27US-OR%27&geometryType=esriGeometryEnvelope&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=FireDiscoveryDateTime%2CInitialLongitude%2CInitialLatitude%2CPOOCounty&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=POOCounty%2CFireDiscoveryDateTime&returnZ=false&returnM=false&resultOffset={}&returnExceededLimitFeatures=true&sqlFormat=standard&f=pjson"

  NIFC_dataset = []

  query_result_offset = 0
  query_result_batch_size = 2000

  # # #

  print("[INFO] Requesting NIFC's ArcGIS Online Feature Service (https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/WFIGS_Incident_Locations/FeatureServer/0/)")

  # Request the first dataset batch with an offset of 0
  request = requests.get(url = NIFC_esri_feature_service_query_base.format(0))
  NIFC_dataset_partition = request.json()

  print("[INFO] Dataset processing begin")

  while len(NIFC_dataset_partition["features"]):
    print("[INFO] Processing dataset batch {} (rows = {})".format(query_result_offset // query_result_batch_size, len(NIFC_dataset_partition["features"])))

    # Iterate over the dataset batch, parsing and storing in memory
    for i in range(0, len(NIFC_dataset_partition["features"])):
      row = parseRow(NIFC_dataset_partition["features"][i])
      NIFC_dataset.append(row)

    query_result_offset += query_result_batch_size

    print("[INFO] Requesting NIFC's ArcGIS Online Feature Service (https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/WFIGS_Incident_Locations/FeatureServer/0/)")

    # Request the (j + 1)'th dataset batch with an offset of "(j + 1) * query_result_batch_size"
    request = requests.get(url = NIFC_esri_feature_service_query_base.format(query_result_offset))
    NIFC_dataset_partition = request.json()

  print("[INFO] Writing data (rows = {})".format(len(NIFC_dataset)))

  fileIO = open("./data/datasets/Oregon_Fire_Incidence.csv", 'w')
  fileIO.write(const.NIFC_data_header)
  fileIO.writelines(NIFC_dataset)
  fileIO.close()

  print("[INFO] Dataset processing complete!")

  return 0

# # #

if __name__ == "__main__":
  sys.exit(main())