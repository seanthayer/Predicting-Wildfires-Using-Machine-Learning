# # #                     # # #
#                             #
# Automatic dataset retrieval #
#                             #
# # #                     # # #

import sys
import os
sys.path.append(os.getcwd())

from datetime import date
import requests
import const as const

# # #

def parseEntry(entry):
  county = entry["attributes"]["POOCounty"]
  fireDate = date.fromtimestamp(entry["attributes"]["FireDiscoveryDateTime"] // 1000) # Rounding division by 1000 to convert UNIX Epoch time from milliseconds to seconds
  foundLng = entry["geometry"]["x"]
  foundLat = entry["geometry"]["y"]
  originLng = entry["attributes"]["InitialLongitude"] if entry["attributes"]["InitialLongitude"] else ''
  originLat = entry["attributes"]["InitialLatitude"] if entry["attributes"]["InitialLatitude"] else ''

  return ",".join([county, str(fireDate.year), str(fireDate.month), str(fireDate.day), str(foundLng), str(foundLat), str(originLng), str(originLat)]) + '\n'

# # #                 # # #
#                         #
# NIFC Dataset Processing #
#                         #
# # #                 # # #

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
    entry = parseEntry(NIFC_dataset_partition["features"][i])
    NIFC_dataset.append(entry)

  query_result_offset += query_result_batch_size

  print("[INFO] Requesting NIFC's ArcGIS Online Feature Service (https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/WFIGS_Incident_Locations/FeatureServer/0/)")

  # Request the (j + 1)'th dataset batch with an offset of "(j + 1) * query_result_batch_size"
  request = requests.get(url = NIFC_esri_feature_service_query_base.format(query_result_offset))
  NIFC_dataset_partition = request.json()

print("[INFO] Writing data (rows = {})".format(len(NIFC_dataset)))

# NOTE: This procedure currently writes data in a format non-conformant with other modules
fileIO = open("./data/datasets/Oregon_Fire_Incidence.csv", 'w')
fileIO.write("County,Year,Month,Day,FoundLng,FoundLat,OriginLng,OriginLat\n")
fileIO.writelines(NIFC_dataset)
fileIO.close()

print("[INFO] Dataset processing complete!")

# # #