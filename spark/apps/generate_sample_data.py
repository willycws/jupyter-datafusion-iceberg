from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from decimal import Decimal
from datetime import date
import random

spark = SparkSession.builder \
    .appName("GenerateSampleData") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "rest") \
    .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000") \
    .config("spark.sql.catalog.iceberg.s3.access-key-id", "minioadmin") \
    .config("spark.sql.catalog.iceberg.s3.secret-access-key", "minioadmin") \
    .config("spark.sql.catalog.iceberg.s3.path-style-access", "true") \
    .getOrCreate()

products = [
    ("Wireless Mouse", Decimal("29.99")),
    ("Mechanical Keyboard", Decimal("89.99")),
    ("USB-C Hub", Decimal("45.50")),
    ("Monitor Stand", Decimal("34.99")),
    ("Webcam HD", Decimal("59.99")),
    ("Laptop Sleeve", Decimal("19.99")),
    ("Desk Lamp", Decimal("24.99")),
    ("Headphones", Decimal("79.99")),
    ("Mouse Pad", Decimal("12.99")),
    ("Cable Organizer", Decimal("9.99")),
]

statuses = ["completed", "completed", "completed", "pending", "cancelled"]

rows = []
for i in range(1, 101):
    product_name, price = random.choice(products)
    rows.append((
        f"ORD-{i:05d}",
        f"CUST-{random.randint(1, 50):04d}",
        product_name,
        random.randint(1, 5),
        price,
        date(2024, random.randint(1, 12), random.randint(1, 28)),
        random.choice(statuses),
    ))

schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("price", DecimalType(10, 2), False),
    StructField("order_date", DateType(), False),
    StructField("status", StringType(), False),
])

df = spark.createDataFrame(rows, schema)
df.writeTo("iceberg.ecommerce.raw_orders").using("iceberg").createOrReplace()

print(f"Inserted {df.count()} rows into iceberg.ecommerce.raw_orders")

spark.stop()
