#encodig=utf-8
# _*_ coding:utf-8 _*_
# Writer : byz
# dateTime : 2016-07-20

# import sys
# sys.path.append('/home/mysql1/spark/bin/python')
# sys.path.append('/home/mysql1/spark/bin/python/pyspark')
# sys.path.append('/home/mysql1/anqu/python/code/Tools')
# reload(sys)
# sys.setdefaultencoding('utf8') 

from pyspark import SparkConf,SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.types import StructType,StructField,IntegerType


conf = SparkConf().setAppName("my_hive")
sc = SparkContext(conf=conf)
hc = HiveContext(sc)
source = sc.parallelize([(-1000,1000)])
# print ''?
schema = StructType([StructField("col1",IntegerType(),False),StructField("col2",IntegerType(),False)])
table = hc.applySchema(source,schema)
table.registerAsTable("temp_table")
rows = hc.sql("select col1 + col2 from temp_table").collect()

sc.stop()

for row in rows:
	print row
