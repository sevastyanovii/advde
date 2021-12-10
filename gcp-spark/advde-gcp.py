from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName('bigquery') \
    .config('spark.jars', 'gs://advde-bucket-1/spark-bigquery-latest_2.12.jar') \
    .getOrCreate()
    
bq_df = spark.read.format('bigquery').option('table','endless-radar-330119.advde_ds_us.myaustin') \
    .option('viewsEnabled','true') \
    .load()
    
print('-------------------------------')
print('-------------------------------')
print(bq_df.printSchema())
print('-------------------------------')
print(f'Count: {bq_df.count()}')

bq_df.write \
    .format('parquet') \
    .mode('overwrite') \
    .save('gs://advde-bucket-1/austin-stats')