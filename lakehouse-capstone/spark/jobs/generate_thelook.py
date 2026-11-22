"""Generate thelook-shaped e-commerce extracts as Parquet in the landing zone.

Each run writes a full extract of five tables, the way many source systems deliver files:
  batch 1  history up to 31 Oct 2026
  batch 2  one more week: new customers, orders and events, older orders moving
           from processing to shipped to complete, and about 3% of customers moving city,
           so incremental models and the snapshot have real work to do.

Randomness comes from hashing ids, so a batch always produces the same rows.

Run inside the Spark container:
  /opt/spark/bin/spark-submit --master "local[2]" /opt/spark/jobs/generate_thelook.py --batch 1
"""
import argparse
from datetime import datetime

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F

START = "2025-01-01 00:00:00"
AS_OF = {1: "2026-10-31 23:59:59", 2: "2026-11-07 23:59:59"}
USERS = {1: 5000, 2: 5300}
ORDERS = {1: 15000, 2: 16500}
PRODUCTS = 300

FIRST = ["Aisha", "Ravi", "Omar", "Mei", "Lucas", "Sara", "Arjun", "Fatima", "Noah", "Priya", "Yuki", "Diego"]
LAST = ["Khan", "Kumar", "Haddad", "Chen", "Silva", "Ahmed", "Rao", "Ali", "Smith", "Iyer", "Sato", "Lopez"]
PLACES = [
    ("New York", "New York", "United States"), ("Austin", "Texas", "United States"),
    ("Sao Paulo", "Sao Paulo", "Brasil"), ("Bogota", "Bogota", "Colombia"),
    ("Shanghai", "Shanghai", "China"), ("Tokyo", "Tokyo", "Japan"), ("Seoul", "Seoul", "South Korea"),
    ("Sydney", "New South Wales", "Australia"), ("Bengaluru", "Karnataka", "India"),
    ("Riyadh", "Riyadh", "Saudi Arabia"), ("London", "England", "United Kingdom"),
    ("Berlin", "Berlin", "Germany"), ("Paris", "Ile-de-France", "France"), ("Madrid", "Madrid", "Spain"),
]
CATEGORIES = ["Jeans", "Tops & Tees", "Sweaters", "Outerwear & Coats", "Shorts", "Active", "Accessories"]
BRANDS = ["Northwind", "Contoso", "Fabrikam", "Tailspin", "Litware", "Adatum"]
TRAFFIC = ["Search", "Organic", "Email", "Facebook", "Display"]
BROWSERS = ["Chrome", "Safari", "Firefox", "Edge"]
FLOW = ["home", "department", "product", "cart", "purchase"]


def num(col: Column, salt: str, mod: int) -> Column:
    """Deterministic pseudo-random integer in [0, mod)."""
    return F.pmod(F.hash(col, F.lit(salt)), F.lit(mod))


def pick(col: Column, salt: str, items: list) -> Column:
    return F.element_at(F.array(*[F.lit(x) for x in items]), (num(col, salt, len(items)) + 1).cast("int"))


def ts(seconds: Column) -> Column:
    return F.timestamp_seconds(seconds)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="/landing/thelook")
    parser.add_argument("--batch", type=int, choices=[1, 2], default=1)
    args = parser.parse_args()
    b = args.batch

    spark = SparkSession.builder.appName(f"generate_thelook_batch_{b}").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")  # like a BigQuery export

    start = F.unix_timestamp(F.lit(START))
    as_of1 = F.unix_timestamp(F.lit(AS_OF[1]))
    as_of = F.unix_timestamp(F.lit(AS_OF[b]))
    span1 = int((datetime.fromisoformat(AS_OF[1]) - datetime.fromisoformat(START)).total_seconds())
    span2 = 7 * 86_400

    # ---------------------------------------------------------------- users
    uid = F.col("id")
    moved = (F.lit(b) == 2) & (num(uid, "moved", 100) < 3)
    place = F.when(moved, num(uid, "place_v2", len(PLACES))).otherwise(num(uid, "place", len(PLACES))) + 1
    places = [F.array(*[F.lit(p[i]) for p in PLACES]) for i in range(3)]
    users = (
        spark.range(1, USERS[b] + 1)
        .select(
            uid,
            pick(uid, "first", FIRST).alias("first_name"),
            pick(uid, "last", LAST).alias("last_name"),
            (18 + num(uid, "age", 53)).cast("long").alias("age"),
            pick(uid, "gender", ["F", "M"]).alias("gender"),
            F.element_at(places[0], place.cast("int")).alias("city"),
            F.element_at(places[1], place.cast("int")).alias("state"),
            F.element_at(places[2], place.cast("int")).alias("country"),
            pick(uid, "traffic", TRAFFIC).alias("traffic_source"),
            ts(F.when(uid <= USERS[1], start - num(uid, "signup", 31_536_000))
                .otherwise(as_of1 + num(uid, "signup2", 43_200))).alias("created_at"),
        )
        .withColumn("email", F.concat(F.lower("first_name"), F.lit("."), F.lower("last_name"),
                                      F.col("id").cast("string"), F.lit("@example.com")))
    )

    # ---------------------------------------------------------------- products
    pid = F.col("id")
    cost = F.round(5 + num(pid, "cost", 9_500) / 100, 2)
    products = (
        spark.range(1, PRODUCTS + 1)
        .select(
            pid,
            pick(pid, "category", CATEGORIES).alias("category"),
            pick(pid, "brand", BRANDS).alias("brand"),
            pick(pid, "dept", ["Women", "Men"]).alias("department"),
            cost.alias("cost"),
            F.round(cost * (1.4 + num(pid, "markup", 120) / 100), 2).alias("retail_price"),
            F.substring(F.md5(pid.cast("string")), 1, 12).alias("sku"),
            (num(pid, "dc", 10) + 1).cast("long").alias("distribution_center_id"),
        )
        .withColumn("name", F.concat_ws(" ", "brand", "category", F.col("id").cast("string")))
    )

    # ---------------------------------------------------------------- orders
    oid = F.col("order_id")
    created = F.when(oid <= ORDERS[1], start + num(oid, "ts", span1)).otherwise(as_of1 + num(oid, "ts2", span2))
    age_h = (as_of - F.col("created_s")) / 3600
    r = num(oid, "status", 100)
    status = (F.when(age_h < 24, "Processing")
              .when(r < 5, "Cancelled")
              .when(age_h < 72, "Shipped")
              .when(r < 15, "Returned")
              .otherwise("Complete"))
    shipped_s = F.col("created_s") + 43_200 + num(oid, "ship", 43_200)
    delivered_s = shipped_s + 86_400 * (1 + num(oid, "deliver", 2))
    orders = (
        spark.range(1, ORDERS[b] + 1).withColumnRenamed("id", "order_id")
        .withColumn("created_s", created)
        .withColumn("status", status)
        .select(
            oid,
            F.when(oid <= ORDERS[1], num(oid, "user", USERS[1]) + 1)
             .otherwise(num(oid, "user2", USERS[b]) + 1).cast("long").alias("user_id"),
            F.col("status"),
            pick(oid, "gender", ["F", "M"]).alias("gender"),
            ts(F.col("created_s")).alias("created_at"),
            ts(F.when(F.col("status").isin("Shipped", "Complete", "Returned"), shipped_s)).alias("shipped_at"),
            ts(F.when(F.col("status").isin("Complete", "Returned"), delivered_s)).alias("delivered_at"),
            ts(F.when(F.col("status") == "Returned", delivered_s + 172_800)).alias("returned_at"),
            (1 + num(oid, "items", 4)).cast("long").alias("num_of_item"),
        )
    )

    # ---------------------------------------------------------------- order items
    items = (
        orders.select("*", F.explode(F.sequence(F.lit(1), F.col("num_of_item").cast("int"))).alias("n"))
        .withColumn("id", F.col("order_id") * 10 + F.col("n"))
        .withColumn("product_id", (num(F.col("id"), "product", PRODUCTS) + 1).cast("long"))
        .join(products.select(F.col("id").alias("product_id"), "retail_price"), "product_id")
        .select(
            "id", "order_id", "user_id", "product_id",
            F.col("id").alias("inventory_item_id"),
            "status", "created_at", "shipped_at", "delivered_at", "returned_at",
            F.round(F.col("retail_price") * (0.8 + num(F.col("id"), "discount", 21) / 100), 2).alias("sale_price"),
        )
    )

    # ---------------------------------------------------------------- events (five per order session)
    events = (
        orders.select("order_id", "user_id", "created_at",
                      F.posexplode(F.array(*[F.lit(x) for x in FLOW])).alias("pos", "event_type"))
        .select(
            (F.col("order_id") * 10 + F.col("pos") + 1).alias("id"),
            "user_id",
            (F.col("pos") + 1).cast("long").alias("sequence_number"),
            F.concat(F.lit("s-"), F.col("order_id").cast("string")).alias("session_id"),
            ts(F.unix_timestamp("created_at") - (4 - F.col("pos")) * 90).alias("created_at"),
            pick(F.col("user_id"), "browser", BROWSERS).alias("browser"),
            pick(F.col("order_id"), "traffic", TRAFFIC).alias("traffic_source"),
            F.concat(F.lit("/"), F.col("event_type")).alias("uri"),
            "event_type",
        )
    )

    for name, df in [("users", users), ("products", products), ("orders", orders.drop("created_s")),
                     ("order_items", items), ("events", events)]:
        path = f"{args.out}/{name}"
        df.coalesce(1).write.mode("overwrite").parquet(path)
        print(f"{name:<12} {spark.read.parquet(path).count():>9,} rows -> {path}")

    spark.stop()


if __name__ == "__main__":
    main()
