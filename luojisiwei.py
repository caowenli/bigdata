import os
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext, HiveContext

sc = SparkContext.getOrCreate()
sql_context = HiveContext(sc)
from pyspark.sql.functions import *

path = "luojisiwei.parquet"
t = sql_context.read.parquet(path)

# # 数据预处理
#
#
# # 数据类型，正负样本比率，数据数量。数据缺失量
# # 查看数据相关信息
# print("查看几行数据")
# print(t.show(5))
#
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
# # 正负样本比例
# a = t.where("label=0").count() / t.count()
# b = t.where("label=1").count() / t.count()
# print(a / b)
# print("负样本的比率", a)
# print("正样本的比率", b)
# print(t.where("label=0").count() / t.count())
# print(t.where("label=1").count() / t.count())

# 数据类型变换
# print(t.dtypes)
# t = t.withColumn('requestId', t['requestId'].astype('int'))
# print(t.dtypes)
print(t.columns)
#数据行筛选
t=t.limit(20)
print(t.count())
t=t.filter(t.requestId !="")
t=t.filter(t.requestId!="null")
print(t.count())
t=t.distinct()
print(t.count())
t=t.dropDuplicates(subset=['UUID'])
print(t.count())
t=t.select('requestId',"label")

# 数据缺失处理
#
# for i in t.columns:
#     t = t.withColumn(i, when(t[i] == "null", None).when(t[i].isNull(), None).otherwise(t[i]))
#     print("数据的属性")
#     print(t[i])
#     print("缺失量")
#     print(t[[i]].dropna().count())
#     print("缺失比率")
#     print(1 - t[[i]].dropna().count() / t.count())
# 对于先知平台（能不能开发一个knn进行数据填充）
# 直接删除>0.8      cpro   ulikelabel  ulikesrc  ulikekey udislikelabel  udislikesrc  udislikekey
# 对于数据缺失>0.4   ckey  cAge   cLike  cDislike
# 数据填充
# 如果数据量很多直接删除行。
# clike cdislike 填充数据0
# t = t.fillna(0, subset='cLike')
# t = t.fillna(0, subset="cDislike")
# t = t.dropna()

# 特征提取

# coding: UTF-8
# input script according to definition of "run" interface
from pyspark.sql.types import *
from pyspark.sql.functions import udf
import re
import math

#
# # 用于计算ctr、当前音频在音频流中的位置，方式是计算比例a/b，
# def divide_number(a, b):
#     if a is None or b is None:
#         return 0
#     return int((a + 1.0) / (b + 1.0) * 100000.0) / 100000.0
#
#
# # 用于计算用户当前看的音频标题用户以前看过的几个、当前音频关键词用户以前看过几个，方式是统计文本特征a在词袋b中出现的个数
# def trans_words(t, bag):
#     if bag is None or t is None:
#         return 0
#     bag_words = re.split(";|丨|,", bag)
#     wordset = [i.strip() for i in bag_words if i]
#     wordset = set(wordset)
#     t_words = re.split(";|丨|,", t)
#     count = 0
#     for w in t_words:
#         if w.strip() in wordset:
#             count += 1
#     return count
#
#
# # 主函数，计算衍生特征
# def run(t1):
#     divide_number_udf = udf(divide_number, DoubleType())
#     trans_words_udf = udf(trans_words, IntegerType())
#     t2 = t1
#     t2 = t2.withColumn('pRankRatio', divide_number_udf(t2['pRank'], t2['pCount'])).withColumn('userCtr',
#                                                                                               divide_number_udf(
#                                                                                                   t2['userClick'], t2[
#                                                                                                       'userView'])).withColumn(
#         'contentCtr', divide_number_udf(t2['contentClick'], t2['contentView']))
#     t2 = t2.withColumn('titleOccured', trans_words_udf(t2['cTitle'], t2['cTitleAll']))
#     t2 = t2.withColumn('keyOccured', trans_words_udf(t2['cKey'], t2['cKeyAll']))
#     return [t2]
#
# t=run(t)
#
