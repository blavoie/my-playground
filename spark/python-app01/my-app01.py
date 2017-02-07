from pyspark import SparkContext, SparkConf, SQLContext

appName = "my-app01"
conf = SparkConf().setAppName(appName)

sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

sqlCtx = SQLContext.getOrCreate(sc)

data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data)

print("Nombre d'elements: " + str(distData.count()))
print("Maximum: " + str(distData.max()))
