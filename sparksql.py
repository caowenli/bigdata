from pyspark.sql import SparkSession
from pyspark.sql.types import Row,StructField,StringType,StructType
import pandas as pd
#列表
spark_session=SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option","some-value").getOrCreate()
l=[('Alice',1)]
spark_session.createDataFrame(l,["name","age"]).collect()
#字典
d=[{'name':'Alice',"age":1}]
spark_session.createDataFrame(d).collect()

#RDD
from pyspark import SparkContext,SparkConf
conf = SparkConf().setMaster('local').setAppName('My App')
sc = SparkContext(conf = conf)
rdd=sc.parallelize([('Alice',1)])
spark_session.createDataFrame(rdd,['name','age']).collect()
#Row
from pyspark.sql import Row
Person=Row('name','age')
rdd=sc.parallelize([('Alice',1)]).map(lambda r:Person(*r))
spark_session.createDataFrame(rdd,['name','age']).collect()
#schema
from pyspark.sql.types import StructType,StructField,IntegerType
schema=StructType([StructField("name",StringType(),True),StructField("age",IntegerType(),True)])
rdd=sc.parallelize([("Alice",1)])
spark_session.createDataFrame(rdd,schema).collect()
rdd=sc.parallelize([('Alice',1)])
spark_session.createDataFrame(rdd,"a:string,b:int").collect()



lines=sc.textFile("people.txt")
#ROW
people=lines.map(lambda l:l.split(",")).map(lambda p:Row(name=p[0],age=int(p[1])))
peopledf=spark_session.createDataFrame(people)
peopledf.show()
#schema
people=lines.map(lambda l:l.split(",")).map(lambda p:Row(name=p[0],age=int(p[1].strip())))
schema=StructType([StructField("name",StringType(),True),StructField("age",IntegerType(),True)])
spark_session.createDataFrame(people,schema).show()


#DataFrame
df=pd.DataFrame({'a':[1,3,5],'b':[4,5,6]})
spark_session.createDataFrame(df).collect()



#通用数据加载
df=spark_session.read.format('json').load("people.json")
#专用加载数据
spark_session.read.json("peopel.json")
spark_session.read.text("people.txt")


#从hive表中加载数据

#数据保存
df.write.format("text").save("data.txt")
df.write.csv("data.csv")


#function






