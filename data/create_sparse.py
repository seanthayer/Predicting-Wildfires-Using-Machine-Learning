import sys
import os
sys.path.append(os.getcwd())

import datetime as date
import pandas as pd
import const as const

def build_data():
    county_list = const.county_list
    data = {'County':[], 'Date':[], 'Temperature':[], 'Precipitation':[], 'Is_Burned':[], 'X':[],
            'Y':[], 'FireDiscoveryDateTime':[], 'InitialLatitude':[], 'InitialLongitude':[],
            'IncidentSize':[], 'FireCause':[]}
    df = pd.DataFrame(data)
    for county in county_list:
        climate_df = pd.read_csv("./data/Merged_data/{}_Climate.csv".format(county))
        counter = 0
        for year in range(2010, 2024):
            for month in range(1, 13):
                if year == 2023 and month > 1:
                    continue
                else:
                    year_str = str(year)
                    if month < 10:
                        month_str = str(0) + str(month)
                    else:
                        month_str = str(month)
                    date_str = year_str + month_str
                    temp = climate_df.iloc[counter, 2]
                    prec = climate_df.iloc[counter, 3]
                    df.loc[len(df.index)] = [county, date_str, temp, prec, 0, '','','','','','','']
                    counter += 1
    df.to_csv('./data/flags.csv')

def insert_flag():
    flag_df = pd.read_csv('./data/flags.csv')
    flag_df = flag_df.drop(columns='Unnamed: 0', axis=1)
    fire_df = pd.read_csv('./data/vectors.csv')
    for idx, record in fire_df.iterrows():
        county = record['POOCounty']
        time = pd.to_datetime(record['FireDiscoveryDateTime'])
        year_str = str(time.year)
        if (time.month < 10):
            month_str = str(0) + str(time.month)
        else:
            month_str = str(time.month)
        fireDate = int(year_str+month_str)
        if fireDate <= 202301:
            index = flag_df[(flag_df['County'].isin([county]))&(flag_df['Date'].isin([fireDate]))].index[0]
            if flag_df.iloc[index, 4] == 0:
                flag_df.iloc[index, 4] = 1
                flag_df.iloc[index, 5] = record["X"]
                flag_df.iloc[index, 6] = record["Y"]
                flag_df.iloc[index, 7] = record["FireDiscoveryDateTime"]
                flag_df.iloc[index, 8] = record["InitialLatitude"]
                flag_df.iloc[index, 9] = record["InitialLongitude"]
                flag_df.iloc[index, 10] = record["IncidentSize"]
                flag_df.iloc[index, 11] = record["FireCause"]
            else:
                f = flag_df.loc[index]
                df_add = pd.DataFrame({'County':[f["County"]],
                                        'Date':[f["Date"]], 
                                        'Temperature':[f["Temperature"]], 
                                        'Precipitation':[f["Precipitation"]], 
                                        'Is_Burned':[1], 
                                        'X':[record["X"]],
                                        'Y':[record["Y"]], 
                                        'FireDiscoveryDateTime':[record["FireDiscoveryDateTime"]], 
                                        'InitialLatitude':[record["InitialLatitude"]], 
                                        'InitialLongitude':[record["InitialLongitude"]],
                                        'IncidentSize':[record["IncidentSize"]], 
                                        'FireCause':[record["FireCause"]]})
                df1 = flag_df.iloc[:index, :]
                df2 = flag_df.iloc[index:, :]
                flag_df = pd.concat([df1, df_add, df2], ignore_index=True)

    flag_df.to_csv('./data/sparse_data.csv')

build_data()
insert_flag()