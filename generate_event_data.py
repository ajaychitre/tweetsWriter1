import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, posexplode, rand
from pyspark.sql.types import *
from delta import configure_spark_with_delta_pip

# ---------- CONFIGURATION ----------
NUM_USERS = 50
#NUM_USERS = 5_000_000
TOTAL_EVENTS = 10000 
#TOTAL_EVENTS = 7_000_000_000
EVENTS_PER_USER_MIN = 100
EVENTS_PER_USER_MAX = 5000
NUM_PARTITIONS = 500  # Adjust depending on cluster/memory
#OUTPUT_PATH = "file:///Users/ajay/hack/events"
OUTPUT_PATH = "s3a://ajays-hackathon-bucket/datalake/events"  # or "file:///tmp/events"
USE_S3 = OUTPUT_PATH.startswith("s3a://")

EVENT_NAMES = (
    ["LOGGED_IN", "LOGGED_OUT"] +
    [f"VISITED_PAGE_{i}" for i in range(1, 21)] +
    [f"CLICKED_BUTTON_SUBMIT_PAGE_{i}" for i in range(1, 21)]
)

DATE_START = datetime(2010, 1, 1)
DATE_END = datetime(2025, 6, 30)

# ---------- INITIALIZE SPARK ----------
builder = (
    SparkSession.builder
    .appName("Generate Event Data")
    .config("spark.sql.shuffle.partitions", "800")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
)

if USE_S3:
    builder = builder \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# ---------- HELPER FUNCTIONS ----------
def random_date():
    delta = DATE_END - DATE_START
    random_days = random.randint(0, delta.days)
    return DATE_START + timedelta(days=random_days, seconds=random.randint(0, 86400))

def generate_batch(start_user_id, end_user_id, events_per_user_range):
    """
    Generate batch of data for a range of user_ids
    """
    data = []
    event_id_counter = start_user_id * events_per_user_range[1]

    for user_id in range(start_user_id, end_user_id):
        num_events = random.randint(*events_per_user_range)
        for _ in range(num_events):
            data.append((
                event_id_counter,
                user_id,
                random.choice(EVENT_NAMES),
                random_date()
            ))
            event_id_counter += 1
    return data

# ---------- MAIN GENERATION LOOP ----------
schema = StructType([
    StructField("event_id", LongType(), False),
    StructField("user_id", IntegerType(), False),
    StructField("event_name", StringType(), False),
    StructField("event_creation_date", TimestampType(), False),
])

users_per_partition = NUM_USERS // NUM_PARTITIONS
events_generated = 0

for partition in range(NUM_PARTITIONS):
    start_user_id = partition * users_per_partition
    end_user_id = (partition + 1) * users_per_partition if partition < NUM_PARTITIONS - 1 else NUM_USERS
    print(f"Generating users {start_user_id} to {end_user_id}...")

    batch_data = generate_batch(start_user_id, end_user_id, (EVENTS_PER_USER_MIN, EVENTS_PER_USER_MAX))
    events_generated += len(batch_data)

    df = spark.createDataFrame(batch_data, schema=schema)
    df = df.repartition(200).persist()

    # Write to Delta Lake format, partitioned by user_id
    df.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("user_id") \
        .save(OUTPUT_PATH)

    print(f"Written partition {partition + 1}/{NUM_PARTITIONS}, total events so far: {events_generated:,}")

print(f"✅ Finished generating ~{events_generated:,} events.")

# ---------- CREATE DELTA TABLE (optional, for SQL) ----------
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS events
    USING DELTA
    LOCATION '{OUTPUT_PATH}'
""")

spark.stop()

