# Databricks notebook source
from delta.tables import DeltaTable
spark
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run "/Workspace/Users/abhirajshri99@gmail.com/SportsPipeline/Data_Processing/utilities"

# COMMAND ----------

dbutils.widgets.text("data_source", "Catalog", "Data Source")
dbutils.widgets.text("Table", "table", "table")

# COMMAND ----------

base_path="/Volumes/playfever/rawingestion/playfeverrawdata/full_load/gross_price/"
gross_price_df=spark.read.format("csv")\
.option("header",True)\
.option("inferschema", True)\
.option("badRecordsPath", "/Volumes/playfever/rawingestion/playfeverrawdata/full_load/products/bad_records")\
.load(base_path)
gross_price_df.show()
gross_price_df.describe()
gross_price_df.write.format("delta").mode("overwrite").saveAsTable("`sportbizz_parent`.bronze.dim_pf_gross_price")
gross_price_df.dropDuplicates()

# COMMAND ----------

gross_price_filter1=gross_price_df
gross_price_filter1.withColumn("product_id", trim(col("produt_id"))).withColumn("month", trim(col("month"))).withColumn("gross_price", trim(col("gross_price")))
badProductidDf=gross_price_df.filter(~col("product_id").rlike(r"^\d{8}$"))
gross_price_filter1=gross_price_filter1.filter(col("product_id").rlike(r"^\d{8}$"))
gross_price_filter1.show()


# COMMAND ----------

grossFilter2=gross_price_filter1.select("*")
grossFilter2=grossFilter2.withColumn("gross_price", when(col("gross_price").rlike(r"\d+"), col("gross_price")).otherwise("Unknown"))

#lets assume we talked to business and they agreed to convert all negatives to positives
grossFilter2=grossFilter2.withColumn("gross_price", regexp_replace(col("gross_price"), r"-", ""))
grossFilter2.show()

# COMMAND ----------

grossFilter3=grossFilter2.select("*")
grossFilter3=grossFilter3.withColumn("gross_price", trim(col("gross_price")))
grossFilter3=grossFilter3.withColumn("month", regexp_replace(col("month"), r" ",r""))
# grossFilter3.select("month").distinct().show()

#Got allowed date formats from business

grossFilter3=grossFilter3.withColumn("month"
                                     , coalesce(try_to_date(col("month"), "yyyy-MM-dd"),
                                                try_to_date(col("month"), "dd-MM-yyyy"),
                                                try_to_date(col("month"), "dd/MM/yyyy"),
                                                try_to_date(col("month"), "yyyy/MM/dd")))

grossFilter3=grossFilter3.withColumn("gross_price", when(col("gross_price").rlike("Unknown"), "0").otherwise(col("gross_price")))
grossFilter3.show()

# COMMAND ----------

productFilter3=spark.read.table("sportbizz_parent.silver.dim_pf_products")
#productFilter3.show()

grossFilter4=grossFilter3.join(productFilter3, on="product_id", how="left")
# print(grossFilter4.count())
# print(grossFilter3.count())

grossFilter4=grossFilter4.filter(col("product_code").isNotNull())
#grossFilter4.show(grossFilter4.count())
grossFilter4=grossFilter4.select("product_code", "product_id", "month", "gross_price")
grossFilter4.write.format("delta").mode("overwrite").saveAsTable("`sportbizz_parent`.silver.dim_pf_gross_price")
grossFilter4.show()


# COMMAND ----------

#GOLD Processing
grossFilter5=grossFilter4.select("*")
from pyspark.sql.window import Window
w=Window.partitionBy(year(col("month"))).orderBy(col("month").desc())
grossFilter5=grossFilter5.withColumn("Rank", rank().over(w))
grossFilter5=grossFilter5.filter(col("Rank")==1)
grossFilter5=grossFilter5.withColumn("year", year(col("month")))
#grossFilter5.show()

grossFilter5=grossFilter5.select("product_code", "product_id", "month", "year", "gross_price")
grossFilter5.show()

# COMMAND ----------

grossFilter6=grossFilter5.select("*")
grossFilter6=grossFilter6.withColumn("gross_price", col("gross_price").alias("price_inr"))
grossFilter6=grossFilter6.withColumn("price_inr", col("gross_price").cast("bigint"))

grossFilter6=grossFilter6.select("product_code", "price_inr", "year")
grossFilter6.write.format("delta").mode("overwrite").saveAsTable("`sportbizz_parent`.gold.pf_dim_gross_price")

source=spark.table("`sportbizz_parent`.gold.pf_dim_gross_price")
target=DeltaTable.forName(spark, "`sportbizz_parent`.gold.dim_gross_price")
target.alias("t").merge(source.alias("s"), "t.product_code=s.product_code").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()