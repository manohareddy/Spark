from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
import json

spark = SparkSession \
        .builder \
        .appName("Assignment 2") \
        .getOrCreate()

sc = spark.sparkContext
#####WRITE YOUR CODE HERE######

words = sc.textFile('assignment2/data').map(json.loads)\
        .flatMap(lambda x:x['text'].strip().lower().split())\
        .map(lambda y:(y,1))\
        .reduceByKey(lambda i,j:i+j)\
        .filter(lambda m:m[1]>=10)\

freq = words.count()

w= words.filter(lambda x:x[0]=='congress' or x[0]=='football' or x[0]=='washington' or x[0]=='london').collect()

high_freq_monthly={}

monsoon_freq={}

for i in range(1,13):
    j =sc.textFile('assignment2/data/2012-'+str('%.2d'%i)+'*')\
           .map(json.loads)\
           .flatMap(lambda x:x['text'].strip().lower().split())\
           .map(lambda x:(x,1))\
           .reduceByKey(lambda x,y:x+y)
    high_freq_monthly.update({i: j.sortBy(lambda y:y[1]).collect()[-1]})
    monsoon_freq.update({i:j.filter(lambda x:x[0]=='monsoon').collect()[0][1]})

mar01 = sc.textFile('assignment2/data/2012-03-01')\
           .map(json.loads)\
           .flatMap(lambda x:x['text'].strip().lower().split())\
           .distinct() 

aug01 = sc.textFile('assignment2/data/2012-08-01')\
           .map(json.loads)\
           .flatMap(lambda x:x['text'].strip().lower().split())\
           .distinct()

exclusive_words_mar1= mar01.subtract(aug01).collect()

print 'total word count for 366 files :\n '+str(freq)
print '\n word frequency of congress, football, washington, london in all files :\n '+str(w)
print '\n monthly highest frequent word (format-(month:(word, frequency))) :\n '+str(high_freq_monthly)
print '\n \n words that occured only on 2012-03-01 but not on 2012-08-01 :\n '+str(exclusive_words_mar1)
print '\n \n frequency of monsoon every month (format-(month : frequency)) :\n '+str(monsoon_freq)

 
