from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions as sf
from datetime import datetime
from pyspark.sql.functions import col , udf
from pyspark.sql.types import DateType
from pyspark.sql.functions import current_date, datediff, unix_timestamp
from pyspark.sql import functions as F

conf = (SparkConf()
        .setAppName("ParseDat")
        .setMaster("local"))
sc = SparkContext(conf = conf)
sqlcontext = SQLContext(sc)

#drdd= sc.textFile("file:///C:/Users/Administrator/Downloads/data1/data1.csv")
customSchema1 = StructType([StructField("year", IntegerType(), True), StructField("month", IntegerType(), True),StructField("day", IntegerType(), True), StructField("day_of_week", IntegerType(), True), \
    StructField("dep_time", IntegerType(), True),StructField("crs_dep_time", IntegerType(), True), StructField("arr_time", IntegerType(), True),StructField("crs_arr_time", IntegerType(), True),\
    StructField("unique_carrier", StringType(), True),StructField("flight_num ", IntegerType(), True),StructField("tail_num", StringType(), True),StructField("actual_elapsed_time ", IntegerType(), True),\
    StructField("crs_elapsed_time", IntegerType(), True),StructField("air_time", IntegerType(), True),StructField("arr_delay", StringType(), True),\
    StructField("dep_delay", IntegerType(), True), StructField("origin", StringType(), True),StructField("dest", StringType(), True),StructField("distance ", IntegerType(), True),\
    StructField("taxi_in ", IntegerType(), True),StructField("taxi_out", IntegerType(), True),StructField("cancelled", IntegerType(), True),StructField("cancellation_code", StringType(), True),
    StructField("diverted", IntegerType(), True),StructField("carrier_delay", StringType(), True),StructField("weather_delay", StringType(), True),StructField("nas_delay", StringType(), True),
    StructField("security_delay", StringType(), True),StructField("late_aircraft_delay", StringType(), True)
    ])

df = sqlcontext.read \
    .format('com.databricks.spark.csv') \
    .options(header='true') \
    .load("file:///C:/Users/Administrator/Downloads/data1/data.csv", schema = customSchema1)
#df.show(10)

customSchema2 = StructType([StructField("name", StringType(), True), StructField("country", StringType(), True), \
    StructField("area_code", IntegerType(), True), StructField("code", StringType(), True)])

df1 = sqlcontext.read \
    .format('com.databricks.spark.csv') \
    .options(header='true') \
    .load("file:///C:/Users/Administrator/Downloads/data1/airport.csv", schema = customSchema2)
#df1.show()
newjoindf = df.join(df1, df.origin == df1.code)


sqlcontext.registerDataFrameAsTable(newjoindf, "jointable")
sqlquery= "SELECT code as Airport, year, month , avg(CAST(arr_delay AS BIGINT)) AS avgdelay FROM jointable GROUP BY code,year,month ORDER BY avgdelay DESC LIMIT 5"
sqlquery1= "SELECT code as Airport, year, month , avg(CAST(arr_delay AS BIGINT)) AS avgdelay FROM jointable GROUP BY code,year,month ORDER BY avgdelay LIMIT 5"
top5airport = sqlcontext.sql(sqlquery).show()
last5airport = sqlcontext.sql(sqlquery1).show()

concatedate = newjoindf.withColumn('date',sf.concat(sf.col('year'),sf.lit('/'), sf.col('month'),sf.lit('/'), sf.col('day')))
func =  udf (lambda x: datetime.strptime(x, '%Y/%m/%d'), DateType())
date = concatedate.withColumn('date', func(col('date')))
sqlcontext.registerDataFrameAsTable(date, "newtable")

sqlquery2= "SELECT DISTINCT code AS airport,date FROM newtable WHERE arr_delay <> 'NA' ORDER BY date DESC"


"SELECT DISTINCT code AS airport, date  FROM newtable WHERE arr_delay IS NOT NULL ORDER BY date DESC "
newdf = sqlcontext.sql(sqlquery2).show(100)
last7days = newdf.where(datediff(current_date(), col("date")) < 7)

