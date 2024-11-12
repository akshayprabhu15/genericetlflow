# Databricks notebook source
from etl_pipeline import Etl 

config_file_path = dbutils.widgets.get("config_file_path")
etl = Etl(spark,config_file_path)
etl.load_bronze()
