from __future__ import print_function,division
import os
import sys
import copy
import functools

from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType

conf = SparkConf().setMaster("yarn").setAppName("autofe").set('spark.yarn.queue', 'solution')
# set app resources
configs = [('spark.driver.memory', '10g'),
     ('spark.executor.memory', '4g'),
     ('spark.executor.instances', '10'),
     ('spark.executor.cores', '2')]
conf.setAll(configs)
# conf = SparkConf().set('master', 'local')
sc = SparkContext.getOrCreate(conf=conf)
# sc = SparkContext.getOrCreate()
sql_context = HiveContext(sc)





#action表进行预处理
#数据加载
path = "hdfs://m7-model-hdp01:8020/user/2-6-0-model-test/user_1/nodes/data-load-load-240240/out/20190717/DAG_36240/NODE_240240/SLOT_0/DataLoad/02150359716"
t = sql_context.read.parquet(path)


#查看数据信息
print("查看几行数据")
print(t.show(5))
print("查看数据类型")
print(t.dtypes)
print(t.schema)
print(t.printSchema())
print("查看数据的列")
print(t.columns)
print("数据描述性统计")
print(t.describe().show())
print("数据的总数")
print(t.count())
print(t.distinct().count())
#数据类型转换
t1=t
t1 = t1.withColumn("user_id", t1["user_id"].cast("Int")).withColumn("model_id", t1["model_id"].cast("Int")).withColumn("order_amt", t1["order_amt"].cast("Int"))
#数据缺失处理
t1=t1.drop('order_amt')
#数据去重复
t1=t1.distinct()
#数据选择
from pyspark.sql.functions import to_date
t1=t1.filter(to_date(t1.time) > '2016-02-01')
t1=t1.filter(to_date(t1.time)<'2016-04-15')


#用户表处理
#数据加载
path = "hdfs://m7-model-hdp01:8020/user/2-6-0-model-test/user_1/nodes/data-load-load-240240/out/20190717/DAG_36240/NODE_240240/SLOT_0/DataLoad/02150359716"
t = sql_context.read.parquet(path)


#查看数据信息
print("查看几行数据")
print(t.show(5))
print("查看数据类型")
print(t.dtypes)
print(t.schema)
print(t.printSchema())
print("查看数据的列")
print(t.columns)
print("数据描述性统计")
print(t.describe().show())
print("数据的总数")
print(t.count())
print(t.distinct().count())

#数据缺失处理
t=t.fillna(value=2,subset='age')
print(t[['age']].count())
#数据类型变换
from pyspark.sql.functions import *

t=t.withColumn('age',when(t.age=='15岁以下',0).when(t.age=='16-25岁',1).when(t.age=='26-35岁' ,3).when(t.age=='36-45岁',4).when(t.age=='46-55岁',5).when(t.age=='56岁以上',6).otherwise(-1))




#商品表进行预处理
path="hdfs://m7-model-hdp01:8020/user/2-6-0-model-test/user_1/nodes/data-load-load-239741/out/20190714/DAG_36137/NODE_239741/SLOT_0/DataLoad/07472958604"
t = sql_context.read.parquet(path)

#查看数据信息
print("查看几行数据")
print(t.show(5))
print("查看数据类型")
print(t.dtypes)
print(t.schema)
print(t.printSchema())
print("查看数据的列")
print(t.columns)
print("数据描述性统计")
print(t.describe().show())
print("数据的总数")
print(t.count())
print(t.distinct().count())
t=t.drop('att1')
