# coding=utf-8
import sys
from pyspark import StorageLevel, SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime
from collections import OrderedDict
from pyspark.sql.window import Window
import pyspark.sql.functions as func

########################  predefined arguments  ###########################
# sparkURL = 'spark://10.100.0.72:7077'

'''
name_string = "Test_Calculate"
conf = SparkConf() \
    .setAppName(name_string) \
    .set("spark.cassandra.connection.host", "10.100.0.212,10.100.0.44") \
    .set("spark.submit.deployMode", "client") \
    .set("spark.driver.maxResultSize", "4g") \
    .set("spark.debug.maxToStringFields", 1000) \
    .set("spark.cores.max", 9) \
    .set("spark.executor.cores", 2) \
    .set("spark.driver.cores", 1) \
    .set("spark.driver.memory", "4g") \
    .set("spark.executor.memory", "4g") \
    .set("spark.default.parallelism", 120) \
    .set("spark.sql.shuffle.partitions", 150) \
    .set("spark.storage.memoryFraction", 0.8) \
    .set("spark.shuffle.memeoryFraction", 0.2) \
    .set("spark.cassandra.input.split.size_in_mb", 64) \
    .set("spark.cassandra.input.fetch.size_in_rows", 100000)
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
'''
reload(sys)
sys.setdefaultencoding('utf-8')

for i in range(1,4):
    print '##########  ',i,':  #########','  hehe  '
