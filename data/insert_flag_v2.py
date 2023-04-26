import pandas as pd

def get_County_List():
    county_list = ["Baker", "Benton", "Clackamas", "Clatsop", "Columbia", "Coos", "Crook", "Curry", "Deschutes",
                    "Douglas", "Gilliam", "Grant", "Harney", "Hood River", "Jackson", "Jefferson", "Josephine",
                    "Klamath", "Lake", "Lane", "Lincoln", "Linn", "Malheur", "Marion", "Morrow", "Multnomah",
                    "Polk", "Sherman", "Tillamook", "Umatilla", "Union", "Wallowa", "Wasco", "Washington",
                    "Wheeler", "Yamhill"]
    return county_list

def build_data():
    county_list = get_County_List()
    data = {'County':[], 'Date':[], 'Temperature':[], 'Precipitation':[], 'Is_Burned':[]}
    df = pd.DataFrame(data)
    for county in county_list:
        climate_df = pd.read_csv("./Merged_data/{}_Climate.csv".format(county))
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
                    df.loc[len(df.index)] = [county, date_str, temp, prec, 0]
                    counter += 1
    df2 = df
    data = {'Temperature': []}
    data2 = {'Precipitation': []}
    prev_temp_1 = pd.DataFrame(data)
    prev_temp_1.loc[0] = -1
    tempdf = df2.drop(['County', 'Date', 'Precipitation', 'Is_Burned'], axis=1)
    prev_temp_1 = pd.concat([prev_temp_1, tempdf], axis=0, ignore_index=True)
    prev_temp_1.rename(columns={'Temperature': 'Prev 1 Temp'})
    df['Prev 1 Temp'] = prev_temp_1

    prev_temp_2 = pd.DataFrame(data)
    prev_temp_2.loc[0] = -1
    prev_temp_2.loc[1] = -1
    prev_temp_2 = pd.concat([prev_temp_2, tempdf], axis=0, ignore_index=True)
    prev_temp_2.rename(columns={'Temperature': 'Prev 2 Temp'})
    df['Prev 2 Temp'] = prev_temp_2

    prev_prec_1 = pd.DataFrame(data2)
    prev_prec_1.loc[0] = -1
    tempdf = df2.drop(['County', 'Date', 'Temperature', 'Is_Burned', 'Prev 1 Temp', 'Prev 2 Temp'], axis=1)
    prev_prec_1 = pd.concat([prev_prec_1, tempdf], axis=0, ignore_index=True)
    prev_prec_1.rename(columns={'Temperature': 'Prev 1 Prec'})
    df['Prev 1 Prec'] = prev_prec_1

    prev_prec_2 = pd.DataFrame(data2)
    prev_prec_2.loc[0] = -1
    prev_prec_2.loc[1] = -1
    prev_prec_2 = pd.concat([prev_prec_2, tempdf], axis=0, ignore_index=True)
    prev_prec_2.rename(columns={'Temperature': 'Prev 2 Prec'})
    df['Prev 2 Prec'] = prev_prec_2

    for idx, row in df.iterrows():
        if int(row['Date']) < 201401:
            df = df.drop(index=idx)

    df = df[['County', 'Date', 'Temperature', 'Prev 1 Temp', 'Prev 2 Temp', 'Precipitation', 'Prev 1 Prec', 'Prev 2 Prec', 'Is_Burned']]

    df.to_csv('flags_v2.csv')

def insert_flag_v2():
    flag_df = pd.read_csv('flags_v2.csv')
    flag_df = flag_df.drop(columns='Unnamed: 0', axis=1)

    fire_df = pd.read_csv('vectors.csv')
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
            flag_df.iloc[index, 8] = 1

    flag_df.to_csv('training_v2.csv')

insert_flag_v2()