import datetime
from requests import get
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
    url = ('https://api.data.gov.sg/v1/environment/psi?date=%s') % each_day
    print("FileNo -->",i,"URL-->",url)
    #file_name = ("psi_%s.json" % each_day)
    file_name = ("/home/flume/itd354project/input_psi_files/psi_%s.json" % each_day)
    download(url, file_name)
print("Files Downloaded")

