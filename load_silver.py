# Databricks notebook source
from etl_pipeline import Etl

config_file_path = dbutils.widgets.get("config_file_path")

etl = Etl(spark)
etl.load_config(config_file_path)
etl.load_silver()




# COMMAND ----------

from DataQualityinTransit import DataValidator

validator = DataValidator(spark)
validator.load_config(config_file_path)
validator.dataqualitychecks()
