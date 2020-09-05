import datetime
from requests import get
import json  
import glob
import pandas as pd  
from pandas.io.json import json_normalize  
from pandas import ExcelWriter
from pandas import ExcelFile
#from datetime import datetime
number_of_days = [1500]

def download(url, file_name):
    # open in binary mode
    with open(file_name, "wb") as file:
        # get request
        response = get(url)
        # write to file
        file.write(response.content)
    
for i in range(0, number_of_days[0]):
    each_day = ((datetime.date.today() - datetime.timedelta(i)).isoformat())
    url = ('https://api.data.gov.sg/v1/environment/rainfall?date=%s') % each_day
    #print(url)
    file_name = ("/home/flume/itd354project/input_rainfall_files/rainfall_%s.json" % each_day)
    download(url, file_name)

