from clickhouse_driver import Client
import pandas as pd

client = Client(host='130.61.143.82', settings={'use_numpy': True})

"""Отслеживание появление новых файлов в своём бакете на AWS S3. Представим, что пользователь или провайдер данных будет загружать новые исторические данные по поездкам в Ваш бакет;
При появлении нового файла запускается оператор импорта данных в созданную таблицу базы данных Clickhouse;
Необходимо сформировать таблицы с ежедневными отчётами по следующим критериям:
– количество поездок в день
– средняя продолжительность поездок в день
– распределение поездок пользователей, разбитых по категории «gender»
Данные статистики необходимо загрузить на специальный S3 бакет с хранящимися отчётами по загруженным файлам.
"""

client.execute('DROP TABLE IF EXISTS advdedb.ride')
client.execute(
    """
CREATE TABLE advdedb.ride (
	  ride_id             String
	, rideable_type       String
	, started_at          DateTime
	, ended_at            DateTime
	, start_station_name  String
	, start_station_id    String
	, end_station_name    String
	, end_station_id      String
	, start_lat           Float64
	, start_lng           Float64
	, end_lat             Float64
	, end_lng             Float64
	, member_casual       String 
 ) ENGINE = MergeTree ORDER BY (started_at)
 """)

data = pd.read_csv('C:\\Users\\vanio\\temp\\JC-202201-citibike-tripdata.csv')
print('inserted', client.insert_dataframe(
    'INSERT INTO advdedb.ride VALUES',
    pd.DataFrame(data)
))
