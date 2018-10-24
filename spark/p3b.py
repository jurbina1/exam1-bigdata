#Code here
spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("/Users/jady.urbina/Desktop/exam1-sp17-bigdata-desc-master/hive/escuelasPR.csv", header=False, inferSchema=True)
sch = df.filter(df._c0 == 'Arecibo').groupBy(df._c1, df1._c2).count()
sch.show()
