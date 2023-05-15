# # #          # # #
#                  #
# Global constants #
#                  #
# # #          # # #

# # #                                      # # #
#                                              #
# This file is tagged: [PENDING DOCUMENTATION] #
#                                              #
# # #                                      # # #

# Global file tags: [PENDING DOCUMENTATION], [DEFUNCT], [WORK IN PROGESS]

NOAA_header_line_offset = 3

NOAA_dataset_query_year_start = 2014
NOAA_dataset_query_year_stop = 2022

NOAA_county_code_list = ["001", "003", "005", "007", "009", "011", "013", "015", "017", "019", "021", "023",
                         "025", "027", "029", "031", "033", "035", "037", "039", "041", "043", "045", "047",
                         "049", "051", "053", "055", "057", "059", "061", "063", "065", "067", "069", "071"]

# # #

county_list = ["Baker", "Benton", "Clackamas", "Clatsop", "Columbia", "Coos", "Crook", "Curry", "Deschutes",
               "Douglas", "Gilliam", "Grant", "Harney", "Hood River", "Jackson", "Jefferson", "Josephine",
               "Klamath", "Lake", "Lane", "Lincoln", "Linn", "Malheur", "Marion", "Morrow", "Multnomah",
               "Polk", "Sherman", "Tillamook", "Umatilla", "Union", "Wallowa", "Wasco", "Washington",
               "Wheeler", "Yamhill"]

month_list = ["January", "February", "March", "April", "May", "June", "July",
              "August", "September", "October", "November", "December"]

# # #

earth_radius = 3958.75587 # In miles, swappable with other units of measure (e.g., km)
info_unit_distance = "mi" # Modify to match units of measurement used in "earth_radius"

oregon_northmost_lat = 46.3
oregon_southmost_lat = 42.0
oregon_eastmost_lng = -116.46666667
oregon_westmost_lng = -124.63333333

# # #

grid_unit_size = 30 # Units correspond to the measurement used for Earth's radius

unit_grid_data_header = "GridX,GridY,GridCenterLng,GridCenterLat,OpenMeteoLng,OpenMeteoLat,County,Year,Month,FireFoundLng,FireFoundLat,FireOriginLng,FireOriginLat,Precipitation,Temperature_Mean_Max,Temperature_Mean,Temperature_Mean_Min,Wind_Speed_Mean_Max,Wind_Gusts_Mean_Max,Evapotranspiration_Mean,FireOccurrence\n"
unit_grid_data_entry_empty = ",,,,,,,,,,,,,,,,,,,,\n"

# # #

NOAA_data_header = "County,Year,Month,{}\n"
NIFC_data_header = "County,Year,Month,Day,FireFoundLng,FireFoundLat,FireOriginLng,FireOriginLat\n"
incident_matrix_data_header = "County,Year,Month,FireFoundLng,FireFoundLat,FireOriginLng,FireOriginLat,Precipitation,Temperature_Mean,Temperature_Max,Temperature_Min\n"