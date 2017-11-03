from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('WordCount')
sc = SparkContext(conf=conf)

t1=sc.textFile('textfile.txt')
print t1.collect()

f1=t1.flatMap(lambda x:x.split())
print f1.collect()

m1=f1.map(lambda x: (x,1))
print m1.collect()

r1=m1.reduceByKey(lambda x,y:x+y)
print r1.collect()

r1.saveAsTextFile('output')
