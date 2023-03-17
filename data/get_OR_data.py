import sys
import os
sys.path.append(os.getcwd())

import datetime as date
import pandas as pd
import const as const

def get_OR_records():
    df = pd.read_csv("Wildland_Fire_Incident_Locations.csv", low_memory=False)
    df_clear = df[(df['POOState'] == 'US-OR') & (df['IncidentTypeCategory'] == 'WF')]
    df_column = df_clear[['X', 'Y', 'FireDiscoveryDateTime', 'InitialLatitude', 'InitialLongitude', 
                        'POOCounty', 'IncidentSize', 'FireCause']]
    df_column.to_csv("./data/Oregon_Fire_Record.csv")



get_OR_records()