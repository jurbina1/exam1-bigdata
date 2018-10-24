#Code here
spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("/Users/jady.urbina/Desktop/exam1-sp17-bigdata-desc-master/hive/escuelasPR.csv", header=False, inferSchema=True)
df1 = spark.read.csv("/Users/jady.urbina/Desktop/exam1-sp17-bigdata-desc-master/hive/studentsPR.csv", header=False, inferSchema=True)
mst = df1.filter(df1._c5 == 'M')
sch = df.filter(df._c5 == 'Superior').filter((df._c2 == 'Ponce')|(df._c2 == 'San Juan'))
jdf = mst.join(sch, mst._c2== sch._c3)
jdf.show()
