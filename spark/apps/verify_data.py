from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("VerifyData") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "rest") \
    .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000") \
    .config("spark.sql.catalog.iceberg.s3.access-key-id", "minioadmin") \
    .config("spark.sql.catalog.iceberg.s3.secret-access-key", "minioadmin") \
    .config("spark.sql.catalog.iceberg.s3.path-style-access", "true") \
    .getOrCreate()

df = spark.read.table("iceberg.ecommerce.raw_orders")

print(f"Total rows in iceberg.ecommerce.raw_orders: {df.count()}")
print("Schema:")
df.printSchema()
print("Sample data:")
df.show(10, truncate=False)

spark.stop()
