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

base_path="/Volumes/playfever/rawingestion/playfeverrawdata/full_load/products/products.csv"
products_df=spark.read.format("csv")\
.option("header",True)\
.option("inferschema", True)\
.option("badRecordsPath", "/Volumes/playfever/rawingestion/playfeverrawdata/full_load/products/bad_records")\
.load(base_path)


products_df.show(truncate=False)
products_df.write.format("delta").mode("overwrite").saveAsTable("`sportbizz_parent`.bronze.dim_pf_products")
products_df.distinct().count()

# COMMAND ----------

productsFilter1=products_df.select("*")
productsFilter1=productsFilter1.dropDuplicates()
productsFilter1=productsFilter1.withColumn("product_name", trim(col("product_name"))).withColumn("category", trim(col("category")))
productsFilter1=productsFilter1.withColumn("product_name", regexp_replace(col("product_name"), r"SportsBar", "PlayFever"))
productsFilter1=productsFilter1.withColumn("product_name", regexp_replace(col("product_name"), r"\s+", " "))
productsFilter1=productsFilter1.withColumn("category", regexp_replace(col("category"), r"\s+", " "))
productsFilter1.show(truncate=False)

# COMMAND ----------

productFilter2=productsFilter1.withColumn("variant", regexp_extract(col("product_name"), r"\((.+)\)",1)).orderBy("product_id","product_name")
productFilter2=productFilter2.withColumn("product_name", regexp_replace(col("product_name"), r"\((.+)\)",""))
productFilter2=productFilter2.withColumn("product_code", sha2(col("product_id").cast("string"),256))
BadProductIdDF=productFilter2.filter(~col("product_id").rlike(r"\d{8}"))
productFilter2=productFilter2.filter(col("product_id").rlike(r"\d{8}"))

BadProductIdDF.show(truncate=False)
productFilter2.show(truncate=False)

#Talk to business regarding BadProductIdDF. Doesn't follow the pattern.


# COMMAND ----------

productFilter3=productFilter2.select("*")
productFilter3=productFilter3.withColumn("product_name", initcap(col("product_name")))
productFilter3=productFilter3.withColumn("category", initcap(col("category")))

#productFilter3.select("product_name").distinct().show(truncate=False)

productFilter3=productFilter3.withColumn("product_name", regexp_replace(col("product_name"), "Protien","Protein"))
productFilter3=productFilter3.withColumn("category", regexp_replace(col("category"), "Protien","Protein"))
productFilter3.show(truncate=False)
productFilter3.count()


# COMMAND ----------

#business provides details for division column
from itertools import chain
divisionValues={
    "Energy Bars" : "Nutrition Bars",
    "Protein Bars": "Nutrition Bars",
    "Granola & Cereals": "Breakfast Foods",
    "Recovery Dairy": "Recovery & Dairy",
    "Healthy Snacks": "Healthy Snacks",
    "Electrolyte Mix": "Hydration & Electrolytes"
}

mapping = create_map([lit(x) for x in chain(*divisionValues.items())])
productFilter3=productFilter3.withColumn("division", mapping[col("category")])
productFilter3.show()

# COMMAND ----------

productFilter3.write.format("delta").mode("overwrite").saveAsTable("`sportbizz_parent`.silver.dim_pf_products")

# COMMAND ----------

#gold
productFilter4=productFilter3.select("product_code", "division", "category", col("product_name").alias("product"),"variant")
productFilter4.write.format("delta").mode("overwrite").saveAsTable("`sportbizz_parent`.gold.pf_dim_products")
productFilter4.show()

#merging
target = DeltaTable.forName(
    spark,
    "`sportbizz_parent`.gold.dim_products"
)

source = spark.read.table(
    "`sportbizz_parent`.gold.pf_dim_products"
)

target.alias("t").merge(
    source.alias("s"),
    "t.product_code = s.product_code"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# COMMAND ----------

productFilter4.count()