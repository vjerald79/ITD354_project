import sys
import datetime
from requests import get
import json
import glob
import pandas as pd
from pandas.io.json import json_normalize
from pandas import ExcelWriter
from pandas import ExcelFile
file_list = glob.glob('/home/flume/itd354project/input_temperature_files/temperature_*.json')
i=0
for file in file_list:
    i+=1
    with open(file) as f:
        d = json.load(f)
    try:
        if i == 1:
            temperature_df = json_normalize(d['items'],record_path=['readings'],meta=['timestamp'])
            temperature_df['timestamp']= pd.to_datetime(temperature_df['timestamp'].str.split('+').str[0])
            temperature_df['timestamp'] = temperature_df['timestamp'].dt.strftime('%Y-%m-%d %H')
            temperature_df_new=temperature_df.groupby(['timestamp','station_id'],as_index=False).value.mean()
        else :
            temperature_df_com = json_normalize(d['items'],record_path=['readings'],meta=['timestamp'])
            temperature_df_com['timestamp']= pd.to_datetime(temperature_df_com['timestamp'].str.split('+').str[0])
            temperature_df_com['timestamp'] = temperature_df_com['timestamp'].dt.strftime('%Y-%m-%d %H')
            temperature_df_new_com=temperature_df_com.groupby(['timestamp','station_id'],as_index=False).value.mean()
            temperature_df_new=temperature_df_new.append(temperature_df_new_com)
    except:
            print("Error!-->File Name:",file, sys.exc_info()[0], "occurred.")
station_df=pd.read_csv("station_region.csv")
temperature_df_new_region=temperature_df_new.merge(station_df,left_on='station_id', right_on='Station_ID').reindex(columns=['timestamp', 'station_id', 'value', 'Region'])
temperature_df_new_region=temperature_df_new_region[temperature_df_new_region['Region'] !='Noregion']
temperature_df_new_region_grouped=temperature_df_new_region.groupby(['Region','timestamp'],as_index=False).value.mean()
temperature_df_new_region_grouped.shape
temperature_df_new_region_grouped.head(10)
temperature_df_new_region_grouped.to_csv("/home/flume/itd354project/output_temperature_files/Temperature.csv",index=False)

