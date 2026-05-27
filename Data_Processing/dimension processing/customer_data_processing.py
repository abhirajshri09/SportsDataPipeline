# Databricks notebook source
from delta.tables import DeltaTable
spark
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run "/Workspace/Users/abhirajshri99@gmail.com/SportsPipeline/Data_Processing/utilities"

# COMMAND ----------

print(bronze_schema)

# COMMAND ----------

dbutils.widgets.text("data_source", "Catalog", "Data Source")
dbutils.widgets.text("Table", "table", "table")

# COMMAND ----------

base_path="/Volumes/playfever/rawingestion/playfeverrawdata/full_load/customers/customers.csv"
customer_df=spark.read.format("csv")\
.option("header",True)\
.option("inferschema", True)\
.option("badRecordsPath", "/Volumes/playfever/rawingestion/playfeverrawdata/full_load/customers/bad_records")\
.load(base_path)


customer_df.show()
customer_df.write.format("delta").mode("overwrite").saveAsTable("`sportbizz_parent`.bronze.dim_pf_customer")

# COMMAND ----------

customerFilter1=customer_df.select("*").dropDuplicates()
customerValidCities=customerFilter1.select(col("city")).distinct()
customerValidCities.show()
customerFilter2=customerFilter1.select("*")


# COMMAND ----------

column_names=["city","customer_id","customer_name"]
for i in column_names:
    customerFilter2=customerFilter2.withColumn(i,trim(col(i)))

for i in column_names:
    customerFilter2=customerFilter2.withColumn(i,trim(regexp_replace(col(i),"\s+"," ")))
customerFilter2.show()

# COMMAND ----------

city_mapping={"Hyderabadd":"Hyderabad",
              "Hyderabad": "Hyderabad",
              "Hyderbad": "Hyderabad",
              "NewDelhee": "New Delhi",
              "NewDheli": "New Delhi",
              "NewDelhi": "New Delhi",
              "Bengaluruu": "Bengaluru",
              "Bengalore": "Bengaluru",
              "Bengaluru": "Bengaluru"
              }
customerFilter3=customerFilter2.select("*")
# for i in city_mapping:
#    customerFilter3=customerFilter3.withColumn("city", regexp_replace(col("city"),i,city_mapping[i]))

allowed=["Bengaluru", "Hyderabad", "New Delhi"]
customerFilter3=customerFilter3.replace(city_mapping, subset=["city"])
customerFilter3.show()

# COMMAND ----------

customerFilter4=customerFilter3.withColumn("city",when(col("city").isNull(), None).otherwise(col("city")))
customerFilter4=customerFilter4.withColumn("customer_name", when(col("customer_name").isNull(), None).otherwise(initcap(col("customer_name")))).orderBy(col("customer_name"))
customerFilter4=customerFilter4.withColumn("city", when(col("city").isin(allowed), col("city")).otherwise(None))
customerFilter4.show(40)

# COMMAND ----------

NullCities=customerFilter4.select("customer_id", "customer_name", "city").filter(col("city").isNull())
NullCities.show()

# COMMAND ----------

# only2cities=customerFilter4.select("customer_name","city")
# only2cities=only2cities.groupBy("customer_name").agg(countDistinct("city").alias("distinct cities")).filter(col("distinct cities")==2)

# only2cities.join(NullCities, on="customer_name", how="inner").select(only2cities.customer_name).distinct().show()

# Just did it for learning purpose of sql and spark, concepts came out as union and some other functions to bend table and data according to my convenience

#Also practice to add cities as individual columns

#BTW business will help handle null cases as we are not sure


# COMMAND ----------

business=[
    (789521, "Hyderabad"),
    (789403, "New Delhi"),
    (789420, "Bengaluru"),
    (789603, "Hyderabad")
]
schema=("customer_id", "city")

business_df=spark.createDataFrame(data=business, schema=schema)
business_df=business_df.withColumn("customer_id", col("customer_id").cast("string"))
business_df.show()
#Remember theres one more way to create this DF from dict, study that also

# COMMAND ----------

customerFilter4.createOrReplaceTempView("customer")
business_df.createOrReplaceTempView("business")

# spark.sql("""
#           select c.customer_id,c.customer_name, 
#               case when c.city is null then (select b.city from business b where b.customer_id=c.customer_id)
#            end as city from customer c
#           """).show()


final_customer=customerFilter4.alias("c").join(business_df.alias("b"), on="customer_id", how="left").select(col("c.customer_id"), col("c.customer_name"),coalesce(col("c.city"), col("b.city")).alias("city"))

final_customer.orderBy(col("c.customer_name"),col("customer_id"),col("city")).show()


# COMMAND ----------

#lets do the final writing
final_customer=final_customer.withColumn("market", lit("India")).withColumn("platform", lit("PlayFever"))
final_customer=final_customer.withColumn("customer_name", concat_ws("-", col("customer_name"), lit(col("city"))))
final_customer=final_customer.withColumn("channel", lit("Acquisition"))
final_customer = final_customer.withColumnRenamed("customer_id", "customer_code")
final_customer.show(truncate=False)
final_customer.write.format("delta").mode("overwrite").saveAsTable("`sportbizz_parent`.silver.dim_pf_customer")

# COMMAND ----------

#gold 
final_customer_gold=final_customer.select("customer_code", col("customer_name").alias("customer"), "market", "platform", "channel")
final_customer_gold.write.format("delta").mode("overwrite").saveAsTable("`sportbizz_parent`.gold.pf_dim_customer")
final_customer_gold.show()

# COMMAND ----------

target=DeltaTable.forName(spark, "`sportbizz_parent`.gold.dim_customers")
source=spark.table("`sportbizz_parent`.gold.pf_dim_customer")
target.alias("t").merge(
    source.alias("s"), "t.customer_code = s.customer_code")\
.whenMatchedUpdateAll()\
.whenNotMatchedInsertAll()\
.execute()