import sys
import os
sys.path.append(os.getcwd())

import datetime as date
import pandas as pd
import const as const


def delete_lines(fileName, head, targetFN):
    fin = open(fileName, 'r')
    lines = fin.readlines()
    fout = open(targetFN, 'w')
    adjusted_lines = ''.join(lines[head:])
    fout.write(adjusted_lines)


def clean_CSV():
    county_list = const.county_list
    for county in county_list:
        prec_dir = "./data/Precipitation_By_County/"
        temp_dir = "./data/Temperature_By_County/"
        target_dir = "./data/Precipitation_Temperature/"
        temp_excep = "_Temp.csv"
        prec_excep = "_Prec.csv"
        precFN = prec_dir + county + prec_excep
        tempFN = temp_dir + county + temp_excep
        lines_to_del = 3
        delete_lines(precFN, lines_to_del, target_dir+county+prec_excep)
        delete_lines(tempFN, lines_to_del, target_dir+county+temp_excep)


def merge_Data():
    county_list = const.county_list
    dir = "./data/Precipitation_Temperature/"
    temp_excep = "_Temp.csv"
    prec_excep = "_Prec.csv"
    climate_excep = "_Climate.csv"
    climate_path = "./Merged_Data/"

    for county in county_list:
        precFN = dir + county + prec_excep
        tempFN = dir + county + temp_excep
        climateFN = climate_path + county + climate_excep

        prec_df = pd.read_csv(precFN)
        temp_df = pd.read_csv(tempFN)

        prec_df.rename(columns={'Value':'Precipitation'}, inplace=True)
        temp_df.rename(columns={'Value':"Temperature"}, inplace=True)

        climate_df = temp_df
        climate_df['Precipitation'] = prec_df['Precipitation']/30
        climate_df.to_csv(climateFN)


merge_Data()