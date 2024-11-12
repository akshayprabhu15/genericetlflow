import json
from delta.tables import DeltaTable
import pyspark.sql.functions as F

class Etl:
    def __init__(self,spark, config_file):
        self.spark = spark
        with open(config_file, 'r') as file:
            self.config = json.load(file)
        
        self.read_path = self.config["excel_file_path"]
        self.bronze_table = self.config["bronze_table"]
        self.file_type = self.config["file_type"]
        self.load_strategy = self.config["load_strategy"]
        self.tableName = self.config["tableName"]
        self.business_keys = self.config["business_keys"]

    def load_bronze(self):
        if self.file_type == "excel":
            sheetname = self.config.get("sheetname", [])
            if not sheetname: 
                df = self.spark.read.format("com.crealytics.spark.excel") \
                    .option("header", "true").option("inferSchema", "true") \
                    .load(self.read_path)
                df.write.format("delta").mode("overwrite").saveAsTable(f"bronze.{self.bronze_table}")
            else:
                for sheet in sheetname:
                    df = self.spark.read.format("com.crealytics.spark.excel") \
                        .option("header", "true").option("inferSchema", "true") \
                        .option("dataAddress", f"'{sheet}'!A1") \
                        .load(self.read_path)
                    df.write.format("delta").mode("overwrite").saveAsTable(f"bronze.{sheet}")

        elif self.file_type == "csv":
            df = self.spark.read.format("csv") \
                .option("header", "true").option("inferSchema", "true") \
                .load(self.read_path)
            df.write.format("delta").mode("overwrite").saveAsTable(f"bronze.{self.bronze_table}")

    def scd_type2(self, source_df, target_table_name, primary_keys):
        target_delta_table = DeltaTable.forName(self.spark, target_table_name)
        target_df = self.spark.sql(f"SELECT * FROM {target_table_name}")

        metadata_cols = ['Start_Date', 'End_Date', 'IsCurrent','JobRun']
        update_cols = [x for x in target_delta_table.toDF().columns if x not in set(primary_keys + metadata_cols)]

        dup_check_cols = [x for x in target_delta_table.toDF().columns if x not in set(metadata_cols)]
        dup_df = source_df.join(target_df, dup_check_cols, 'inner').select(source_df.columns)
        source_df = source_df.subtract(dup_df)

        merge_condition = ''.join([f"concat(existing.{pk})" for pk in primary_keys])
        update_condition = ' OR '.join([f"existing.{col} <> incoming.{col}" for col in update_cols])

        updates_df1 = source_df.alias('incoming').join(target_df.alias('existing'), primary_keys) \
            .where(f"existing.IsCurrent = 1 AND ({update_condition})") \
            .selectExpr("NULL as mergeKey", "incoming.*")
        updates_df2 = source_df.withColumn("mergeKey", F.concat(*[F.col(c) for c in primary_keys])).select('mergeKey', *source_df.columns)

        delta_df = updates_df1.union(updates_df2).withColumn("Start_Date", F.current_date()) \
            .withColumn("End_Date", F.lit('null')) \
            .withColumn("IsCurrent", F.lit(1))\
            .withColumn("JobRun",F.current_timestamp())

        target_delta_table.alias('existing') \
            .merge(delta_df.alias('incoming'), f"{merge_condition}=mergeKey") \
            .whenMatchedUpdate(condition=f"existing.IsCurrent = 1 AND ({update_condition})",
                               set={"IsCurrent": "0", "End_Date": F.current_date(), "JobRun": F.current_timestamp()}) \
            .whenNotMatchedInsertAll() \
            .execute()

    def upsert(self, source_df, target_table_name, primary_keys):
        target_delta_table = DeltaTable.forName(self.spark, target_table_name)
        target_df = self.spark.sql(f"SELECT * FROM {target_table_name}")

        metadata_cols = ["Created_Date", "Updated_Date"]
        dup_check_cols = [x for x in target_delta_table.toDF().columns if x not in set(metadata_cols)]
        dup_df = source_df.join(target_df, dup_check_cols, 'inner').select(source_df.columns)
        source_df = source_df.subtract(dup_df)

        source_df = source_df.withColumn("Created_Date", F.lit(None)).withColumn("Updated_Date", F.lit(None))
        
        update_cols = [x for x in target_delta_table.toDF().columns if x not in set(primary_keys + metadata_cols)]

        merge_condition = " AND ".join([f"existing.{pk} = incoming.{pk}" for pk in primary_keys])

        update_set = {col: f"incoming.{col}" for col in update_cols}
        update_condition = " OR ".join([f"existing.{col} <> incoming.{col}" for col in update_cols])

        insert_values = {col: f"incoming.{col}" for col in set(primary_keys + update_cols)}

        target_delta_table.alias("existing")\
        .merge(source_df.alias("incoming"),merge_condition)\
        .whenMatchedUpdate(condition=update_condition, set={**update_set,"Updated_Date":F.current_timestamp()})\
        .whenNotMatchedInsert(values={**insert_values,"Created_Date":F.current_timestamp(),"Updated_Date":F.current_timestamp()}).execute()

    def load_silver(self):
        for table, target_table_name in self.tableName.items():
            source_df = self.spark.read.format("delta").table(f"bronze.{table}")
            primary_keys = self.business_keys.get(table)
            if self.load_strategy == 'SCD-Type2':
                if not self.spark.catalog.tableExists(target_table_name):
                    print(target_table_name)
                    #source_df = spark.read.format("delta").table(f"bronze.{table}")

                    source_df = source_df.withColumn("Start_Date", F.current_date()).withColumn("End_Date", F.lit('null')).withColumn("IsCurrent", F.lit(1)).withColumn("JobRun",F.current_timestamp())
                    source_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(target_table_name)
                else:    
                    self.scd_type2(source_df, target_table_name, primary_keys)
            elif self.load_strategy == 'Upsert':
                if not self.spark.catalog.tableExists(target_table_name):
                    source_df = self.spark.read.format("delta").table(f"bronze.{table}").withColumn("Created_Date", F.current_timestamp()).withColumn("Updated_Date", F.current_timestamp())
                    source_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table_name)
                else:    
                    self.upsert(source_df, target_table_name, primary_keys)