from datetime import datetime, timedelta, date
from decimal import Decimal
import random

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Parameters
DAG_ID = 'Insert_Customer_Order_Data_To_Iceberg'
SPARK_MASTER = 'local[*]'
ICEBERG_CATALOG_URI = 'http://iceberg-rest:8181'
S3_ENDPOINT = 'http://minio:9000'
S3_ACCESS_KEY = 'minioadmin'
S3_SECRET_KEY = 'minioadmin'

ICEBERG_PACKAGES = (
    'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,'
    'org.apache.iceberg:iceberg-aws-bundle:1.5.2'
)


def _get_spark_session(app_name):
    from pyspark.sql import SparkSession
    return SparkSession.builder \
        .appName(app_name) \
        .master(SPARK_MASTER) \
        .config("spark.jars.packages", ICEBERG_PACKAGES) \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "rest") \
        .config("spark.sql.catalog.iceberg.uri", ICEBERG_CATALOG_URI) \
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.iceberg.s3.endpoint", S3_ENDPOINT) \
        .config("spark.sql.catalog.iceberg.s3.access-key-id", S3_ACCESS_KEY) \
        .config("spark.sql.catalog.iceberg.s3.secret-access-key", S3_SECRET_KEY) \
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true") \
        .config("spark.sql.catalog.iceberg.s3.region", "us-east-1") \
        .config("spark.executorEnv.AWS_REGION", "us-east-1") \
        .config("spark.executorEnv.AWS_ACCESS_KEY_ID", S3_ACCESS_KEY) \
        .config("spark.executorEnv.AWS_SECRET_ACCESS_KEY", S3_SECRET_KEY) \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.dir", "/tmp/spark-events") \
        .getOrCreate()


def generate_sample_data():
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType

    spark = _get_spark_session("GenerateSampleData")

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
    for i in range(1, 1000001):
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


def verify_data():
    spark = _get_spark_session("VerifyData")

    df = spark.read.table("iceberg.ecommerce.raw_orders")
    print(f"Total rows in iceberg.ecommerce.raw_orders: {df.count()}")
    print("Schema:")
    df.printSchema()
    print("Sample data:")
    df.show(10, truncate=False)

    spark.stop()


default_args = {
    'owner': 'Willy',
    'start_date': datetime(2024, 1, 1),
    'email': ['test@test.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Seed sample data into Iceberg raw_orders table',
    schedule_interval=None,
    catchup=False,
)

# Create Iceberg namespace via REST catalog
create_namespace = BashOperator(
    task_id='create_namespace',
    bash_command='curl -s -X POST http://iceberg-rest:8181/v1/namespaces '
                 '-H "Content-Type: application/json" '
                 '-d \'{"namespace": ["ecommerce"]}\' || true',
    dag=dag,
)

# Generate sample data and write to Iceberg
generate_sample_data_task = PythonOperator(
    task_id='generate_sample_data',
    python_callable=generate_sample_data,
    dag=dag,
)

# Verify data was inserted
verify_data_task = PythonOperator(
    task_id='verify_data',
    python_callable=verify_data,
    dag=dag,
)

# Task pipeline
create_namespace >> generate_sample_data_task >> verify_data_task
