from datetime import datetime

# Read all json files extracted from raw ingestion
df = spark.read.json("/dbfs/catfacts_express/raw")

print("Showing schema:")
df.printSchema()
print("\n\n")

print("Transforming dataframe - dropping unused columns")
df = (df.drop(df.__v)
      .drop(df.deleted)
      .drop(df.used)
      )

print("Showing transformed schema:")
df.printSchema()
print("\n\n")

print("Showing first five records of dataframe")
df.show(5)
print("\n\n")

try:
    print("Creating dbfs directory")
    dbutils.fs.mkdirs("/dbfs/catfacts_express/consumption")
except Exception as e:
    raise Exception(e)

# Get current datetime to be used to timestamp .parquet file
now_string = datetime.now().strftime("%Y%m%d_%H%M%S")
parquet_path = f"/dbfs/catfacts_express/consumption/catfacts_{now_string}.parquet"
print(f"Writing dataframe into Parquet. Total records: {df.count()}")
print(f"File path: {parquet_path}")
df.write.mode('overwrite').parquet(parquet_path)