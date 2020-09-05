from pandas.io.json import json_normalize
from pandas import ExcelWriter
from pandas import ExcelFile
import pandas as pd
import json  
import glob
psi_df=pd.DataFrame()
file_list = glob.glob('/home/flume/itd354project/input_psi_files/*.json')
i=0
for file in file_list:
    i+=1
    print('FileName',file)
    with open(file) as f:
        d = json.load(f)
    if i == 1:
        psi_df = json_normalize(d['items'])
    else :
        psi_df_com = json_normalize(d['items'])
        psi_df = psi_df.append(psi_df_com)
psi_df.to_csv("/home/flume/itd354project/output_psi_files/PSI_His_PSI_Data.csv")

