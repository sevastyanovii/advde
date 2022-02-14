import pandas as pd
from datetime import datetime

rep = pd.read_csv(r'C:\Users\vanio\temp\rep1_202201.csv')
print(type(rep))
print(rep.info())
print('---------------------------')
print('---------------------------')
print('---------------------------')
# print(rep.head())
# raw_data['Mycol'] =  pd.to_datetime(raw_data['Mycol'], format='%d%b%Y:%H:%M:%S.%f')
print(pd.to_datetime(rep.loc[1]['trip_date']).strftime('%Y_%m'))


