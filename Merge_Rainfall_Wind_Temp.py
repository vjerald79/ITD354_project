import pandas as pd  
from pandas.io.json import json_normalize  
from pandas import ExcelWriter
from pandas import ExcelFile

temperature_df=pd.read_csv("/home/flume/itd354project/output_temperature_files/Temperature.csv")
windspeed_df=pd.read_csv("/home/flume/itd354project/output_wind_files/Wind.csv")
rainfall_df=pd.read_csv("/home/flume/itd354project/output_rainfall_files/Rainfall.csv")


temp_ws_df=temperature_df.merge(windspeed_df,right_on=['timestamp','Region'],left_on=['timestamp','Region'],suffixes=('_temperature', '_windspeed'))
temp_ws_rf_df=rainfall_df.merge(temp_ws_df,right_on=['timestamp','Region'],left_on=['timestamp','Region'])
temp_ws_rf_df.head(10)
temp_ws_rf_df.to_csv("/home/flume/itd354project/output/Rainfall_Wind_Temp.csv",index=False)

