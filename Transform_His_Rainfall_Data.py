import sys
import datetime
from requests import get
import json  
import glob
import pandas as pd  
from pandas.io.json import json_normalize  
from pandas import ExcelWriter
from pandas import ExcelFile
file_list = glob.glob('/home/flume/itd354project/input_rainfall_files/rainfall_*.json') 
i=0
for file in file_list:
    i+=1
    print('FileName-->',i,file)
    with open(file) as f: 
        d = json.load(f)
    try:
        if i == 1:
            rainfall_df = json_normalize(d['items'],record_path=['readings'],meta=['timestamp'])
            rainfall_df['timestamp']= pd.to_datetime(rainfall_df['timestamp'].str.split('+').str[0])
            rainfall_df['timestamp'] = rainfall_df['timestamp'].dt.strftime('%Y-%m-%d %H')
            rainfall_df_new=rainfall_df.groupby(['timestamp','station_id'],as_index=False).value.mean()
        else :
            rainfall_df_com = json_normalize(d['items'],record_path=['readings'],meta=['timestamp'])
            rainfall_df_com['timestamp']= pd.to_datetime(rainfall_df_com['timestamp'].str.split('+').str[0])
            rainfall_df_com['timestamp'] = rainfall_df_com['timestamp'].dt.strftime('%Y-%m-%d %H')
            rainfall_df_new_com=rainfall_df_com.groupby(['timestamp','station_id'],as_index=False).value.mean()
            rainfall_df_new=rainfall_df_new.append(rainfall_df_new_com)
    except:
            print("Error!-->File Name:",file, sys.exc_info()[0], "occurred.")
station_df=pd.read_csv("station_region.csv")
rainfall_df_new_region=rainfall_df_new.merge(station_df,left_on='station_id', right_on='Station_ID').reindex(columns=['timestamp', 'station_id', 'value', 'Region'])
rainfall_df_new_region=rainfall_df_new_region[rainfall_df_new_region['Region'] !='Noregion']
rainfall_df_new_region_grouped=rainfall_df_new_region.groupby(['Region','timestamp'],as_index=False).value.mean()
rainfall_df_new_region_grouped.shape
rainfall_df_new_region_grouped.head(10)
rainfall_df_new_region_grouped.to_csv("/home/flume/itd354project/output_rainfall_files/Rainfall.csv",index=False)

