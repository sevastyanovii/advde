
df = spark.read.option("header", True).option("inferSchema", True)\
.option("dateFormat", 'yyyy-MM-dd').csv('owid-covid-data.csv')


spark.createDataFrame([(5,)], ['a']).select(format_number('a', 4).alias('v'))


df.select('iso_code', to_date(col("date"),"yyyy-MM-dd")\
.alias("statdate"), 'total_cases').filter("statdate <= date'2021-03-31'").filter(df.iso_code.like('OWID%') == False)\
.groupBy("iso_code").agg(max("total_cases").alias('max_total_cases'))\
.sort(col("max_total_cases").desc())\
.select('iso_code', format_number('max_total_cases', 0).alias('max_total_cases'))\
.show(n = 15, truncate=True)

#+--------+---------------+
#|iso_code|max_total_cases|
#+--------+---------------+
#|OWID_WRL|    128,897,696|
#|OWID_EUR|     39,848,358|
#|OWID_NAM|     35,136,720|
#|     USA|     30,462,210|
#|OWID_ASI|     28,507,133|
#|OWID_EUN|     26,786,132|
#|OWID_SAM|     21,149,540|
#|     BRA|     12,748,747|
#|     IND|     12,221,665|
#|     FRA|      4,705,068|
#|     RUS|      4,494,234|
#|     GBR|      4,359,982|
#|OWID_AFR|      4,217,313|
#|     ITA|      3,584,899|
#|     TUR|      3,317,182|
#+--------+---------------+
#only showing top 15 rows


df.select('iso_code', to_date(col("date"),"yyyy-MM-dd").alias("statdate"), 'new_cases')\
.filter("statdate >= date'2021-03-25'").filter("statdate <= date'2021-03-31'").filter(df.iso_code.like('OWID%') == False)\
.groupBy("iso_code").agg(sum('new_cases').alias('sum_new_cases'), max('statdate').alias('on_date'))\
.sort(col("sum_new_cases").desc()).show(n = 10)

#+--------+-------------+----------+
#|iso_code|sum_new_cases|   on_date|
#+--------+-------------+----------+
#|     BRA|     528736.0|2021-03-31|
#|     USA|     448300.0|2021-03-31|
#|     IND|     434131.0|2021-03-31|
#|     FRA|     266069.0|2021-03-31|
#|     TUR|     225900.0|2021-03-31|
#|     POL|     201046.0|2021-03-31|
#|     ITA|     144037.0|2021-03-31|
#|     DEU|     120656.0|2021-03-31|
#|     UKR|      95016.0|2021-03-31|
#|     ARG|      78944.0|2021-03-31|
#+--------+-------------+----------+
#only showing top 10 rows

     
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.functions import when
from pyspark.context import SparkContext

win = Window.orderBy("statdate")

df.select('iso_code', to_date(col("date"),"yyyy-MM-dd").alias("statdate"), 'new_cases')\
.filter("statdate >= date'2021-03-25'").filter("statdate <= date'2021-03-31'").filter("iso_code = 'RUS'")\
.withColumn("new_cases_pre", F.lag(F.col("new_cases"), 1).over(win))\
.select(col("statdate"), col("new_cases").alias("today"), col("new_cases_pre").alias("yesterday"))\
.withColumn("delta", col("today") - col("yesterday"))\
.show()

#+----------+------+---------+------+
#|  statdate| today|yesterday| delta|
#+----------+------+---------+------+
#|2021-03-25|9128.0|     null|  null|
#|2021-03-26|9073.0|   9128.0| -55.0|
#|2021-03-27|8783.0|   9073.0|-290.0|
#|2021-03-28|8979.0|   8783.0| 196.0|
#|2021-03-29|8589.0|   8979.0|-390.0|
#|2021-03-30|8162.0|   8589.0|-427.0|
#|2021-03-31|8156.0|   8162.0|  -6.0|
#+----------+------+---------+------+
#
