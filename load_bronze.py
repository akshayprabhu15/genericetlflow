# Databricks notebook source
from Pytest import DataValidator

config_file_path = dbutils.widgets.get("config_file_path")

datacheck = DataValidator(spark)
datacheck.validate_rawdata(config_file_path)
datacheck.validate_duplicate(config_file_path)

# COMMAND ----------

from etl_pipeline import Etl 

config_file_path = dbutils.widgets.get("config_file_path")
etl = Etl(spark)
etl.load_config(config_file_path)
etl.load_bronze()
