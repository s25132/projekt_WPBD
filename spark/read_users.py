from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T
from decimal import Decimal

TOPIC = "appdb.public.users"

spark = (SparkSession.builder
    .appName("KafkaToMinIO-users")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate())

# MinIO / S3A (jak u Ciebie)
hconf = spark._jsc.hadoopConfiguration()
hconf.set("fs.s3a.endpoint", "http://minio:9000")
hconf.set("fs.s3a.access.key", "minioadmin")
hconf.set("fs.s3a.secret.key", "minioadmin")
hconf.set("fs.s3a.path.style.access", "true")
hconf.set("fs.s3a.connection.ssl.enabled", "false")

# === 1) kafka source ===
df = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load())

raw = df.select(
    F.col("key").cast("string").alias("key_str"),
    F.col("value").cast("string").alias("json")
).filter(F.col("json").isNotNull())  # tombstones out

# === 2) Schemy (wrapped: {"schema":..., "payload":...}) ===
# key: {"schema":...,"payload":{"id":int}}
key_plain  = T.StructType([T.StructField("id", T.IntegerType(), False)])
key_wrapped = T.StructType([
    T.StructField("schema",  T.MapType(T.StringType(), T.StringType()), True),
    T.StructField("payload", key_plain, True)
])

# value payload (before/after)
after_schema = T.StructType([
    T.StructField("id",           T.IntegerType(), False),
    T.StructField("full_name",      T.StringType(), False),
    T.StructField("email", T.StringType(),  False),
    T.StructField("created_at",   T.LongType(),    False),
])

envelope_payload = T.StructType([
    T.StructField("before", after_schema, True),
    T.StructField("after",  after_schema, True),
    T.StructField("op",     T.StringType(), True),
    T.StructField("ts_ms",  T.LongType(),   True),
    T.StructField("source", T.MapType(T.StringType(), T.StringType()), True)
])

envelope_wrapped = T.StructType([
    T.StructField("schema",  T.MapType(T.StringType(), T.StringType()), True),
    T.StructField("payload", envelope_payload, True)
])

# === 3) Parsowanie JSON (wrapped key + wrapped value) ===
parsed = raw.select(
    F.from_json("key_str", key_wrapped).alias("k"),
    F.from_json("json",    envelope_wrapped).alias("e")
).filter(F.col("e.payload").isNotNull())

# === 4) UDF: bytes (big-endian two's complement) -> Decimal(scale=2) ===
SCALE = 2
@F.udf(T.DecimalType(20, SCALE))
def debezium_decimal(b):
    if b is None:
        return None
    n = int.from_bytes(b, byteorder="big", signed=True)
    return Decimal(n).scaleb(-SCALE)  # n / (10**SCALE)

# === 5) Wypłaszczenie + konwersje typów ===
flat = (parsed
    .withColumn("payload",
        F.when(F.col("e.payload.op").isin("c","u","r"), F.col("e.payload.after"))
         .when(F.col("e.payload.op") == "d",            F.col("e.payload.before"))
    )
    .select(
        F.col("k.payload.id").alias("pk_id"),
        F.col("e.payload.op").alias("op"),
        F.col("payload.id").alias("id"),
        F.col("payload.full_name").alias("full_name"),
        F.col("payload.email").alias("email"),
        F.from_unixtime(F.col("payload.created_at")/1_000_000).cast("timestamp").alias("created_at"),
        (F.col("e.payload.ts_ms")/1000).cast("timestamp").alias("event_ts")
    )
    .withColumn("is_deleted", (F.col("op") == "d").cast("boolean"))
)

delta_q = (flat.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "s3a://datalake/checkpoints/users/")
    .option("path", "s3a://datalake/tables/users/")
    .start())

spark.streams.awaitAnyTermination()
