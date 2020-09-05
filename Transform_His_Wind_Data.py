import sys
import datetime
from requests import get
import json
import glob
import pandas as pd
from pandas.io.json import json_normalize
from pandas import ExcelWriter
from pandas import ExcelFile
file_list = glob.glob('/home/flume/itd354project/input_wind_files/wind_*.json')
i=0
for file in file_list:
    i+=1
    with open(file) as f:
        d = json.load(f)
    try:
        if i ==	 1:
            wind_df = json_normalize(d['items'],record_path=['readings'],meta=['timestamp'])
            wind_df['timestamp']= pd.to_datetime(wind_df['timestamp'].str.split('+').str[0])
            wind_df['timestamp'] = wind_df['timestamp'].dt.strftime('%Y-%m-%d %H')
            wind_df_new=wind_df.groupby(['timestamp','station_id'],as_index=False).value.mean()
        else :
            wind_df_com = json_normalize(d['items'],record_path=['readings'],meta=['timestamp'])
            wind_df_com['timestamp']= pd.to_datetime(wind_df_com['timestamp'].str.split('+').str[0])
            wind_df_com['timestamp'] = wind_df_com['timestamp'].dt.strftime('%Y-%m-%d %H')
            wind_df_new_com=wind_df_com.groupby(['timestamp','station_id'],as_index=False).value.mean()
            wind_df_new=wind_df_new.append(wind_df_new_com)
    except:
            print("Error!-->File Name:",file, sys.exc_info()[0], "occurred.")
station_df=pd.read_csv("station_region.csv")
wind_df_new_region=wind_df_new.merge(station_df,left_on='station_id', right_on='Station_ID').reindex(columns=['timestamp', 'station_id', 'value', 'Region'])
wind_df_new_region=wind_df_new_region[wind_df_new_region['Region'] !='Noregion']
wind_df_new_region_grouped=wind_df_new_region.groupby(['Region','timestamp'],as_index=False).value.mean()
wind_df_new_region_grouped.shape
wind_df_new_region_grouped.head(10)
wind_df_new_region_grouped.to_csv("/home/flume/itd354project/output_wind_files/Wind.csv",index=False)

