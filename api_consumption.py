df = spark.read.json("/dbfs/catfacts_express/raw")
print(df)
df = df.drop(df.__v).drop(df.deleted).drop(df.used)
df.show()

try:
  print("Creating dbfs directory")
  dbutils.fs.mkdirs("/dbfs/catfacts_express/consumption")
except Exception as e:
  print(e)
df.count()
df.write.mode('overwrite').parquet("/dbfs/catfacts_express/consumption/catfacts.parquet")