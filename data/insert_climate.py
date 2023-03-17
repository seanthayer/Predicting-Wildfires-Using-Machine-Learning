import sys
import os
sys.path.append(os.getcwd())

import datetime as date
import pandas as pd
import const as const

def insert_climate(fire_record, climate_dir, climate_excep):
    fire_df = pd.read_csv(fire_record)
    fire_df.insert(fire_df.shape[1], 'Temperature', -1)
    fire_df.insert(fire_df.shape[1], 'Precipitation', -1)

    for idx, record in fire_df.iterrows():
        time = pd.to_datetime(record[3])
        year_str = str(time.year)
        if (time.month < 10):
            month_str = str(0) + str(time.month)
        else:
            month_str = str(time.month)
        fireDate = int(year_str+month_str)
        fireCounty = str(record[6])

        climateFN = climate_dir + fireCounty + climate_excep
        climate_df = pd.read_csv(climateFN)

        for i, climate in climate_df.iterrows():
            if fireDate == int(climate[1]):
                fire_df.loc[idx, 'Temperature'] = climate[2]
                fire_df.loc[idx, 'Precipitation'] = climate[3]

    fire_df = fire_df.drop(columns='Unnamed: 0', axis=1)
    fire_df.to_csv("vectors.csv")



insert_climate("./data/Oregon_Fire_Record.csv", "./data/Merged_Data/", "_Climate.csv")