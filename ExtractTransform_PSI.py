import datetime
from requests import get
from datetime import datetime
from pytz import timezone
import json
import pandas as pd
from pandas.io.json import json_normalize
from pandas import ExcelWriter
from pandas import ExcelFile
json_df=pd.DataFrame()
json_df_unique=pd.DataFrame()
#each_day="2020-08-20T01:45:00"
fmt = "%Y-%m-%dT%H:%M:%S"
now_time = datetime.now(timezone('Asia/Singapore'))
new_time=now_time.strftime(fmt)
print("New Time-->",new_time)
url = ('https://api.data.gov.sg/v1/environment/psi?date_time=%s') % new_time
print(url)
file_name = ("/tmp/psi_downloaded_file.json")
response =get(url).content
print("Response-->",response)
output_temp_filename="/home/flume/itd354project/output_psi_files/PSI_Data_Extract.csv"
output_unique_data_filename="/home/flume/itd354project/output_psi_files/PSI_unique_Extract.csv"

response_string = json.loads(response)
json_df=json_normalize(response_string['items'])
#json_df_hdr=pd.DataFrame(data=no_col_names_df, columns=col_names_df.columns)
json_df.to_csv(output_unique_data_filename,mode='a',index=False,header=False)

