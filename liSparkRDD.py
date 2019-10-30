from pyspark import SparkContext
# sc=SparkContext(master='local')
# #查看spark的信息
# print(sc.version)
# print(sc.pythonVer)
# print(sc.master)
# print(str(sc.sparkHome))
# print(str(sc.sparkUser()))
# print(sc.appName)
# print(sc.defaultMinPartitions)
# print(sc.defaultParallelism)
#配置
from pyspark import SparkContext,SparkConf
conf = SparkConf().setMaster('local').setAppName('My App')
sc = SparkContext(conf = conf)

#load Data
rdd=sc.parallelize([('a',7),('a',2),('b',2)])
print(rdd.take(5))
# rdd2=sc.parallelize([('a',2),('d',1),('b',1)])
# rdd3=sc.parallelize(range(1,100,2))
# rdd4=sc.parallelize([("a",["x","y","z"]),("b",["p","r"])])
# textFile=sc.textFile("/Desktop/hello.txt")
# textFile2=sc.wholeTextFiles("/Desktop/hello")
#
# #RDD的函数
# rdd.getNumPartitions()
#
# #统计个数
# print(rdd.count())
# print(rdd.countByKey())
# print(rdd.countByValue())
# print(rdd.collectAsMap())
# print(rdd.sum())
# print(rdd.isEmpty())
# rdd3.sum()
# rdd3.min()
# rdd3.mean()
# rdd3.stdev()
# rdd3.variance()
# rdd3.histogram(3)
# rdd3.stats()
#
# #使用函数
# rdd.map(lambda x:x+(x[1],x[0])).collect()
# rdd5=rdd.flatMap(lambda x:x+(x[1],x[0]))
# rdd5.collect()
# rdd4.flatMapValues(lambda x:x).collect()
#
# rdd.collect()
# rdd.take(2)
# rdd.first()
# rdd.top(2)
# rdd3.sample(False,0.14,seed=81)
# rdd.filter(lambda x:"a" in x).collect()
# rdd5.distinct().collect()
# rdd.keys().collect()
#
# #Iterating
# def g(x):
#     print(x)
# rdd.foreach(g)
#
#
#
#
# #Reshaping Data
#
# rdd.reduceByKey(lambda x,y:x+y).collect()
# rdd.reduce(lambda x,y:x+y)
# rdd3.groupBy(lambda x:x%2).mapValues(list).collect()
# rdd.groupByKey().mapValues(list).collect()
#
# #Aggregating
# seqOp=(lambda x,y:(x[0]+y,x[1]+1))
# combOp=(lambda x,y:(x[0]+y[0],x[1]+y[1]))
# rdd3.aggregate((0,0),seqOp,combOp)
# rdd3.aggregateByKey((0,0),seqOp,combOp)
# rdd3.keyBy(lambda x:x+x).collect()
#
# rdd.subtract(rdd2).collect()
#
# rdd2.subtractByKey(rdd).collect()
# rdd.cartesian(rdd2).collect()
#
#
#
