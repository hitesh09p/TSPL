
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import explode
from pyspark.sql import *
#conf = SparkConf()\
#       .setMaster("local")\
#       .setAppName('spark-hive')

#sc = SparkContext(conf=conf)

#hiveContext = HiveContext(sc)

#hiveContext.sql("create database test")
#hiveContext.sql("use test")
#hiveContext.sql("create table txn1 (txnno INT, txndate STRING, custno INT, amount DOUBLE, category STRING)"
#            " row format delimited Fields terminated by ',' Stored as textfile ")
#hiveContext.sql("create table txn2 (txnno INT, txndate STRING, custno INT, amount DOUBLE, category STRING)"
#            " row format delimited Fields terminated by ',' Stored as textfile ")
#hiveContext.sql("LOAD DATA LOCAL INPATH 'C:/Users/bigdatagurukul/Downloads/txn1' OVERWRITE INTO TABLE txn1")
#hiveContext.sql("LOAD DATA LOCAL INPATH 'C:/Users/bigdatagurukul/Downloads/txn2' OVERWRITE INTO TABLE txn2")
#df1 = hiveContext.sql("select * from txn1 ")
#df2 = hiveContext.sql("select * from txn2")
#df1.show()
#df2.show()
#df3 = df2.subtract(df1).show()

spark = SparkSession.builder\
     .master("local")\
     .appName("spark-avro")\
     .config("avro.mapred.ignore.inputs.without.extension","false")\
     .config("spark.hadoop.avro.mapred.ignore.inputs.without.extension","false")\
     .getOrCreate()

df = spark.read.format("com.databricks.spark.avro").load("C:/Users/bigdatagurukul/Downloads/cvs_reponse_sample_data")
filterdf = df.select("accPlans","basePlanConfiguration","acuteMaintainenceDoseDetail")
#filterdf.show()
#filterdf.printSchema()
df1 = filterdf.withColumn("accPlans", explode(filterdf.accPlans))\
              .withColumn("basePlanConfiguration", explode(filterdf.basePlanConfiguration))\
              .withColumn("acuteMaintainenceDoseDetail",explode(filterdf.acuteMaintainenceDoseDetail))

#df1.printSchema()
#df1.show()
finaldf = df1.selectExpr("accPlans.planCodeCode as accPlans_planCodeCode",\
                  "basePlanConfiguration.apl4medicaredtype as basePlanConfiguration_apl4medicaredtype" , \
                  "basePlanConfiguration.aps1pppriceschedule as basePlanConfiguration_aps1pppriceschedule " )
finaldf.show(100)














