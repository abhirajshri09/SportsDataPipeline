# Databricks notebook source
spark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from delta.tables import DeltaTable

# COMMAND ----------

dbutils.widgets.text("Catalog", "Catalog")
dbutils.widgets.text("Schema", "Schema")
dbutils.widgets.text("Table", "Table")

# COMMAND ----------

#bronze
landing_path=f"/Volumes/playfever/rawingestion/playfeverrawdata/landing/"
incr_orders_df=spark.read.format("csv")\
               .option("header",True)\
               .option("inferSchema",True)\
               .load(landing_path)\
               .select("*", "_metadata.file_name", "_metadata.file_size")
incr_orders_df.write.mode("append").option("mergeSchema", "false").format("delta").saveAsTable("`sportbizz_parent`.bronze.pf_orders")

#staging
incr_orders_df.write.mode("overwrite").format("delta").saveAsTable("`sportbizz_parent`.bronze.staging")


# COMMAND ----------

dbutils.fs.mv("/Volumes/playfever/rawingestion/playfeverrawdata/landing/",
              "/Volumes/playfever/rawingestion/playfeverrawdata/processed/orders/", recurse=True)

dbutils.fs.mkdirs("/Volumes/playfever/rawingestion/playfeverrawdata/landing/")

# COMMAND ----------

orders2_df=spark.read.table("`sportbizz_parent`.bronze.staging")
orders2_df=orders2_df.dropDuplicates()

orders2_df=orders2_df.withColumn("order_id", trim(col("order_id"))).withColumn("order_id", when(col("order_id").rlike(r"^\S{12}$"), col("order_id")).otherwise(r"INVALID"))

orders2_df=orders2_df.withColumn("product_id", trim(col("product_id"))).withColumn("product_id", col("product_id").cast("string")).withColumn("product_id", when(col("product_id").rlike(r"^\d{8}$"), col("product_id")).otherwise(r"INVALID"))

orders2_df=orders2_df.withColumn("customer_id", trim(col("customer_id"))).withColumn("customer_id", when(col("customer_id").rlike("^\d+$"), col("customer_id")).otherwise(r"INVALID"))

orders2_df=orders2_df.withColumn("order_qty", trim(col("order_qty"))).withColumn("order_qty", when(col("order_qty").isNull(), "INVALID").otherwise(col("order_qty")))

#send to business
invalid_customers=orders2_df.filter((col("customer_id").rlike(r"INVALID")) | (col("product_id").rlike(r"INVALID")) | (col("order_id").rlike(r"INVALID")) | (col("order_qty").rlike(r"INVALID")) )


orders2_df = orders2_df.filter(
    (col("order_id") != "INVALID") & 
    (col("product_id") != "INVALID") & 
    (col("customer_id") != "INVALID") &
    (col("order_qty") != "INVALID")
)

orders2_df.show(truncate=False)

# COMMAND ----------

orders3_df=orders2_df.select("*")
orders3_df = orders3_df.withColumn("order_placement_date", trim(col("order_placement_date"))) \
    .withColumn("order_placement_date", 
        regexp_replace(col("order_placement_date"), r"^[A-Za-z]+,\s*", "")) \
    .withColumn("order_placement_date", coalesce(
        expr("try_to_date(order_placement_date, 'MMMM dd, yyyy')"),
        expr("try_to_date(order_placement_date, 'yyyy/MM/dd')"),
        expr("try_to_date(order_placement_date, 'dd-MM-yyyy')"),
        expr("try_to_date(order_placement_date, 'dd/MM/yyyy')"),
        expr("try_to_date(order_placement_date, 'yyyy-MM-dd')"),
        expr("try_to_date(order_placement_date, 'MM/dd/yyyy')")
    ))

#orders3_df.show(orders3_df.count())



product_df=spark.read.table("`sportbizz_parent`.silver.dim_pf_products")
#product_df.show()

orders3_df=orders3_df.join(product_df, on="product_id", how="left").select(orders3_df["*"], product_df["product_code"])

orders3_df=orders3_df.filter(col("product_code").isNotNull())
orders3_df.show()
#orders4_df.count()


# COMMAND ----------

#lets put it in silver
orders3_df.write.mode("append").option("mergeSchema", "false").format("delta").option("enableChangeDataFeed", "True").saveAsTable("`sportbizz_parent`.silver.pf_orders")

#staging
orders3_df.write.mode("overwrite").format("delta").saveAsTable("`sportbizz_parent`.silver.staging")

# COMMAND ----------

#gold
orders4_df=orders3_df.select("customer_id", "product_code", "order_qty", "order_placement_date")
orders4_df = orders4_df \
    .withColumn("order_placement_date", to_date(date_format(col("order_placement_date"), "yyyy-MM-01")))\
    .withColumnRenamed("order_placement_date", "date")\
    .withColumn("customer_id", col("customer_id").cast("double").cast("bigint")) \
    .withColumnRenamed("customer_id", "customer_code") \
    .withColumn("order_qty", col("order_qty").cast("double").cast("bigint")) \
    .withColumnRenamed("order_qty", "sold_quantity")


orders5_df=orders4_df.groupBy("date","customer_code", "product_code").agg(sum("sold_quantity").alias("sold_quantity"))
orders5_df=orders5_df.select("date", "product_code", "customer_code", "sold_quantity")
orders5_df.write.mode("overwrite").format("delta").option("enableChangeDataFeed","True").saveAsTable("`sportbizz_parent`.gold.pf_orders")
orders5_df.show()
orders5_df.count()

# COMMAND ----------

target=DeltaTable.forName(spark ,"`sportbizz_parent`.gold.fact_orders")
source=spark.read.table("`sportbizz_parent`.gold.pf_orders")
target.alias("t").merge(
  source.alias("s"),
  "t.date = s.date AND t.product_code = s.product_code AND t.customer_code = s.customer_code AND t.sold_quantity = s.sold_quantity"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()