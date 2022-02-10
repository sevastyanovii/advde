import pandas as pd
from clickhouse_driver import Client

data = pd.read_csv('C:\\Users\\vanio\\temp\\JC-202201-citibike-tripdata.csv')

client = Client(host='130.61.143.82', settings={'use_numpy': True})

client.insert_dataframe(
    'INSERT INTO advdedb.ride VALUES',
    pd.DataFrame(data)
)
