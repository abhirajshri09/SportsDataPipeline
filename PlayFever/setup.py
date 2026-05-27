# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC drop catalog if exists PlayFever cascade;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG if not exists playfever;