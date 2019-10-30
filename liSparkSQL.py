from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,when
from pyspark.sql.types import Row,StructField,StringType,StructType,IntegerType
import pandas as pd
#
#
#
#
# #数据加载：：：：：：：：：：：：：：：：：：：：：：：：：：：：：：：：：：：：：：：：：：：：：：：：：：：：：:::::::::::::::::::::
# # #列表
spark_session=SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option","some-value").getOrCreate()
# l=[('Alice',1)]
# spark_session.createDataFrame(l,["name","age"]).collect()
#字典
# d=[{'name':'Alice',"age":1}]
# data=spark_session.createDataFrame(d)
#
# #RDD
# from pyspark import SparkContext,SparkConf
# conf = SparkConf().setMaster('local').setAppName('My App')
# sc = SparkContext(conf = conf)
# rdd=sc.parallelize([('Alice',1)])
# spark_session.createDataFrame(rdd,['name','age']).collect()
# #Row
# from pyspark.sql import Row
# Person=Row('name','age')
# rdd=sc.parallelize([('Alice',1)]).map(lambda r:Person(*r))
# spark_session.createDataFrame(rdd,['name','age']).collect()
# #schema
# from pyspark.sql.types import StructType,StructField,IntegerType
# schema=StructType([StructField("name",StringType(),True),StructField("age",IntegerType(),True)])
# rdd=sc.parallelize([("Alice",1)])
# spark_session.createDataFrame(rdd,schema).collect()
# rdd=sc.parallelize([('Alice',1)])
# spark_session.createDataFrame(rdd,"a:string,b:int").collect()
# lines=sc.textFile("people.txt")
#
#
# # ROW
# people=lines.map(lambda l:l.split(",")).map(lambda p:Row(name=p[0],age=int(p[1])))
# peopledf=spark_session.createDataFrame(people)
# peopledf.show()
# #schema
# people=lines.map(lambda l:l.split(",")).map(lambda p:Row(name=p[0],age=int(p[1].strip())))
# schema=StructType([StructField("name",StringType(),True),StructField("age",IntegerType(),True)])
# spark_session.createDataFrame(people,schema).show()
#
#
# #DataFrame
# df=pd.DataFrame({'a':[1,3,5],'b':[4,5,6]})
# data=spark_session.createDataFrame(df)
# for i in data.collect():
#     print(i[0])
# #通用数据加载
# df=spark_session.read.format('json').load("people.json")
# #专用加载数据
# spark_session.read.json("peopel.json")
# spark_session.read.text("people.txt")
# #从hive表中加载数据
#
#
# #数据保存::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
# df.write.format("text").save("data.txt")
# df.write.csv("data.csv")
# df.write.text()
# df.write.parquet()

#使用spark进行数据处理的步骤



#导入相关配置


# import os
# from pyspark import  SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext,HiveContext
sc = SparkContext.getOrCreate()
# sql_context = HiveContext(sc)
# data_path = os.path.join('luojisiwei.parquet')
# t = sql_context.read.format('parquet').load(data_path)
# list1=[]
# list2=[]
# for i in t.collect():
#     list1.append(i)
# print(list1[0])


#数据预处理


# 查看数据相关信息
# print("查看几行数据")
# print(t.show(5))
# print("查看数据类型")
# print(t.dtypes)
# print(t.schema)
# print(t.printSchema())
# print("查看数据的列")
# print(t.columns)
# print("数据描述性统计")
# print(t.describe().show())
# print("数据的总数")
# print(t.count())
# print(t.distinct().count())
# #正负样本比例
#正负样本的比例
# a=t.where("label=0").count()/t.count()
# b=t.where("label=1").count()/t.count()
# print(a/b)
# print("负样本的比率",a)
# print("正样本的比率",b)
# print(t.where("label=0").count()/t.count())
# print(t.where("label=1").count()/t.count())

# coding: UTF-8
# input script according to definition of "run" interface

#数据缺失处理
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql.types import StringType,IntegerType

# t=t.drop('order_amt')
#
#数据去重
# print(t.columns)
# print(t.count())
# print(t.distinct().count())
# print(t.dropDuplicates(subset=['cAge']).count())
# t=t.distinct()
# t=t.dropDuplicates()
# print(t.count())

# print(t.show(5))
# print(t.dtypes)

# # 数据类型变换
# t1 = t1.withColumn("user_id", t1["user_id"].cast("Int")).withColumn("model_id", t1["model_id"].cast("Int")).withColumn(
#     "order_amt", t1["order_amt"].cast("Int"))
# # 数据缺失处理
# t1 = t1.drop('order_amt')
# # 数据去重复
# t1 = t1.distinct()
# # 样本选择
# t1 = t1.filter(to_date(t1.time) > '2016-02-01' )
# t1=t1.filter(to_date(t1.time) < '2016-04-15')

#数据缺失处理
# print(t.columns)
# for i in t.columns:
#     print(i)
#     print(t[[i]].count())
#数据填充的方法
import pyspark.sql.functions
# t.dropna()
# t.fillna()
# from pyspark.sql.functions import mean
# mean_val = t.select(mean(t['cAge'])).collect()
# mean_sales = mean_val[0][0] # to show the number
# t.na.fill(mean_sales,['cAge']).show()

# #数据查询
# from pyspark.sql import functions as F
# print(t.show(5))
# print(t.columns)
# t.select("cAge").show()
# t.select("cId","cAge").show()
# t.select("cId",t["cAge"]+1).show()
# t.select('cId','cAge',2019-t['cAge']>30).show()
# #满足年龄>30返回1，不满足返回0
# t.select("cId",F.when(2019-t['cAge']>30,1).otherwise(0)).show()
# t.select("cId","cAge",t["cAge"].isin(1986,1983)).show()
# t1=t[t["cAge"].isin(1986,1983)]
# t1.show()
# t.select("cId","cTitle",t["cTitle"].like("人工智能")).show()
# t.select(t["cTitle"].substr(1,3).alias("cTitle_1")).show()
# t.select(t["cAge"].between(1986,1999)).show()
# #filter
# t.filter(t['cAge']>1986).show(5)
# t.where(t['cAge']>1986).show(5)
# #groupby
# t.groupBy("cAge").count().show()
# #sort
# t.sort("cAge").show()
# t.orderBy(["cAge"]).show()
# #创建临时视图
# t.createGlobalTempView("lj1")
# t.createTempView("lj2")
# #添加列
# t.withColumn('cAge1',2019-t['cAge'])
# #更新列名
# t=t.withColumnRenamed('cAge1','age')
# t.drop('cAge')

from pyspark.sql.functions import *



#
#
#
# # dataframe方法总结
# #转换操作：
# t.agg().collect()
# t.filter().collect()
# t.groupBy()
# t.orderBy()
# t.sort()
# t.join()
# t.union()
# t.describe()
# t.distinct()
# t.drop()
# t.dropDuplicates()
# t.dropna()
# t.limit()
# t.replace()
# t.fillna()
# t.select()
# t.toDF()
# t.toJSON()
# t.withColumn()
# #动作函数
# t.collect()
# t.first()
# t.head()
# t.show()
# t.take()
# t.corr()
# t.cov()
# t.foreach()
# t.toPandas()
# #Row
# from pyspark.sql import Row
# Person=Row('name','age')
# rdd=sc.parallelize([('Alice',1)]).map(lambda r:Person(*r))
# df=spark_session.createDataFrame(rdd)
# print(df)
# df.select(df['age'].alias())
# df['a'].astype()
# df['b'].between()
# df['a'].isNull()
# df['a'].isNotNull()
# df['a'].isin()
# df['a'].like()
# df['a'].contains()
# df['a'].startswith()
# df['a'].endswith()
# when(df['a']>0).when(df['a']<-1).otherwise(0)
# #groupdata
# df.groupBy().agg()
# df.groupBy().avg()
# df.groupBy().sum()
# df.groupBy().max()
# df.groupBy().min()
#
#
# #functions
# abs()
# #字符串函数
# length()
# translate()
# #日期函数
# #聚合函数
# #排序函数
# #窗口函数
# #when   udf   size
#

