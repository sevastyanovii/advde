import pandas as pd

rep = pd.read_csv(r'C:\Users\vanio\temp\JC-201810-citibike-tripdata.csv')
print(type(rep))
print(rep.info())

print('--------------------------------------------')

print(rep.loc[0])

print('--------------------------------------------')

print('tripduration' in rep.columns)

