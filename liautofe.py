import os
from pyspark import SparkContext
from pyspark.sql import SQLContext,HiveContext
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType
from pyspark.sql import functions as F
import autofe
from autofe.window import window_aggregate,partitionby_aggregate
from autofe.aggregate import groupby_aggregate
from autofe.functions import spark_functions
sc = SparkContext.getOrCreate()
sql_context = HiveContext(sc)

data_path = os.path.join('luojisiwei.parquet')
t = sql_context.read.format('parquet').load(data_path)
print(t.show(5))
print(t.dtypes)
print(t.head())
print(t.first())
print(t.take(2))
print(t.schema)