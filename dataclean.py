from pyspark import SparkContext
from pyspark.sql import SQLContext,SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

#
# # 对字符串进行处理
# def remove_right(x, w='undefined'):
#     if x is None or len(x) <= 0:
#         return x
#     l = len(w)
#     if w == x[-l:]:
#         return x[:-l]
#     else:
#         return x
#
#
# def run(t1, context_string):
#     sc = SparkContext()
#     sqlContext = SQLContext(sc)
#     remove_right_udf = udf(remove_right, StringType())
#     ops = []
#     for i, (col, dtype) in enumerate(t1.dtypes):
#         if dtype == 'string':
#             op = F.trim(F.col(col))
#             if i == len(t1.columns) - 1:
#                 op = remove_right_udf(op)
#         else:
#             op = F.col(col)
#         ops.append(op.alias(col))
#     res = t1.select(*ops)
#     return res
#
#
# # 对数据进行统计分析
# DELIMITER = '_'
#
#
# def remove_right(x, w='undefined'):
#     l = len(w)
#     return x[:-l]
#
#
# def run(t1, context_string):
#     sc = SparkContext()
#     sql_context = SQLContext(sc)
#     cols = t1.columns
#     res = None
#     for fname, func in [('dcnt', F.approxCountDistinct), ('min', F.min), ('max', F.max)]:
#         exprs = []
#         for col in cols:
#             exprs.append(func(F.col(col)).alias(col))
#         if res is None:
#             res = t1.agg(*exprs)
#         else:
#             res = res.unionAll(t1.agg(*exprs))
#     exprs = []
#     for col in cols:
#         exprs.append(F.sum(F.when(F.trim(F.col(col)) == F.lit('')) | (F.isnull(F.col(col))), 1).otherwise(0)
#                      .alias(col))
#     res = res.unionAll(t1.agg(*exprs))
#     stats = res.rdd.map(list).collect()
#     transpose_stats = list(zip(cols, *stats))
#     res = sc.parallelize(transpose_stats).toDF(['col', 'dcnt', 'min', 'max', 'cnt_null']).coalesce(1)
#     return [res]
sc = SparkContext()
sqlContext = SQLContext(sc)
list1=[(20,30)]
# res=sqlContext.createDataFrame(list1,['row1','row2']).collect()
res=sc.parallelize(list1).toDF(['row1','row2']).coalesce(1)
print(res)
print(res)