from pyspark.sql.functions import col,current_timestamp,lit
from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, StringType,IntegerType,DoubleType,BooleanType,TimestampType,MapType,ShortType,LongType
import sys
from pyspark.sql.utils import AnalysisException
import json
import csv
import yaml
import pandas as pd



class Etl:
    def __init__(self,spark: SparkSession):
        self.spark=spark
        self.bronze_table = []
        self.load_strategy = []
        self.read_path = []
        self.file_type = []
        self.tableName = []
        self.business_keys = []
        self.primary_keys = []
        self.multiline = False
        self.header = False
        self.trailing_whitespace = False
        self.leading_whitespace = True
        self.inferSchema = False
        self.load_path_df = None
     
    def load_config(self,file_path:str):
        #Formated to take CSV and JSON Files
        print("Loading Config Data")
        if file_path.endswith('.csv'):
            print("Loading csv file ")
            detail = pd.read_csv(file_path).to_dict(orient='records')
        elif file_path.endswith('.json'):
            print("Loading JSON file")
            with open(file_path, 'r') as file:
                detail = json.load(file)
        elif file_path.endswith('.yaml') or file_path.endswith('.yml'):
            print("Loading YAML file")
            with open(file_path, 'r') as file:
                detail = yaml.safe_load(file)
        else:
            raise ValueError("Unsupported file format")

        for entry in detail:
            if file_path.endswith('.yaml') or file_path.endswith('.yml'):
                self.bronze_table.append(entry.get('bronze_table'))
                self.read_path.append(entry.get('read_path'))
                self.file_type.append(entry.get('file_type'))
                self.load_strategy.append(entry.get('load_strategy'))
                self.primary_keys.append(entry.get('primary_keys', []))
            elif file_path.endswith('.csv'):
                self.bronze_table.append(entry.get('bronze_table'))
                self.read_path.append(entry.get('read_path'))
                self.file_type.append(entry.get('file_type'))
                self.load_strategy.append(entry.get('load_strategy'))
                self.primary_keys.append(entry.get('primary_keys', []).split(','))
            elif file_path.endswith('.json'):
                self.read_path = self.config["excel_file_path"]
                self.bronze_table = self.config["bronze_table"]
                self.file_type = self.config["file_type"]
                self.load_strategy = self.config["load_strategy"]
                self.tableName = self.config["tableName"]
                self.business_keys = self.config["business_keys"]
        print(f"Table Name: {self.bronze_table}")
        print(f"Path: {self.read_path}")
        print(f"Load Strategy: {self.load_strategy}")
        print(f"Primary Key: {self.primary_keys}")
        print(f"File type: {self.file_type}")
        return self.file_type,self.bronze_table,self.read_path,self.load_strategy,self.primary_keys



    def path_data(self,val):
        if val.endswith('.csv'):
            with open(val, 'r', encoding='utf-8') as file:
                reader = csv.reader(file)
        
                # Read the first row to check if it could be a header
                first_row = next(reader, None)
        
                # If there's a first row and it's not empty, assume it's a header
                if first_row and all(isinstance(x, str) for x in first_row):
                    self.header = True
        
                # Now, check for multiline rows and trailing whitespaces
                for row in reader:
                    for field in row:
                        # Check if any field contains newline characters (indicating multiline content)
                        if '\n' in field or '\r' in field:
                            self.multiline = True
                
                        # Check for Leading whitespace
                        if isinstance(field, str) and field != field.lstrip():
                            self.leading_whitespace = True
                
                        # Check for trailing whitespace
                        if isinstance(field, str) and field != field.rstrip():
                            self.trailing_whitespace = True
                
                    if  self.multiline:  # If multiline is detected, exit early
                        break
        
                # After checking for multiline and whitespace, load the CSV with Spark
                df = self.spark.read.format('csv')\
                    .option('header', self.header)\
                        .option('multiline', self.multiline)\
                            .option('ignoreLeadingWhiteSpace',self.leading_whitespace)\
                                .option('ignoreTrailingWhiteSpace',self.trailing_whitespace).load(val)
        
            #Check if its csv  
            self.load_path_df = self.spark.read.format("csv") \
                .option("header", self.header) \
                .option("inferSchema", self.inferSchema) \
                .option("multiLine", self.multiline) \
                .option("ignoreLeadingWhiteSpace", self.leading_whitespace) \
                .option("ignoreTrailingWhiteSpace", self.trailing_whitespace) \
                .load(val)
        return self.load_path_df


    def load_bronze(self):
        for ftype in self.file_type:
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

            elif ftype == "csv":
                print("Loading CSV File")
                for (tb , val ,p) in zip(self.bronze_table,self.read_path, self.primary_keys):
                    #Bronze Table Name
                    b_table_name = "bronze." + tb 
                    #Check if its csv
                    
                    self.load_path_df = self.spark.read.format("csv").option('header', True).load(val)
                    self.load_path_df.write.format("delta").mode("overwrite").saveAsTable(b_table_name)
        


    def scd_type2(self, source_df, target_table_name, primary_keys):
        target_delta_table = DeltaTable.forName(self.spark, target_table_name)
        target_df = self.spark.sql(f"SELECT * FROM {target_table_name}")

        metadata_cols = ['Start_Date', 'End_Date', 'IsCurrent','JobrunID']
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
            .withColumn("JobrunID",F.date_format(F.current_timestamp(), "yyyyMMdd").cast("int"))

        target_delta_table.alias('existing') \
            .merge(delta_df.alias('incoming'), f"{merge_condition}=mergeKey") \
            .whenMatchedUpdate(condition=f"existing.IsCurrent = 1 AND ({update_condition})",
                               set={"IsCurrent": "0", "End_Date": F.current_date(), "JobrunID": F.date_format(F.current_timestamp(), "yyyyMMdd").cast("int")}) \
            .whenNotMatchedInsertAll() \
            .execute()

    def upsert(self, source_df, target_table_name, primary_keys):
        target_delta_table = DeltaTable.forName(self.spark, target_table_name)
        target_df = self.spark.sql(f"SELECT * FROM {target_table_name}")

        metadata_cols = ["Created_Date", "Updated_Date","JobrunID"]
        dup_check_cols = [x for x in target_delta_table.toDF().columns if x not in set(metadata_cols)]
        dup_df = source_df.join(target_df, dup_check_cols, 'inner').select(source_df.columns)
        source_df = source_df.subtract(dup_df)

        source_df = source_df.withColumn("Created_Date", F.lit(None)).withColumn("Updated_Date", F.lit(None)).withColumn("JobrunID", F.date_format(F.current_timestamp(), "yyyyMMdd").cast("int"))
        
        update_cols = [x for x in target_delta_table.toDF().columns if x not in set(primary_keys + metadata_cols)]

        merge_condition = " AND ".join([f"existing.{pk} = incoming.{pk}" for pk in primary_keys])

        update_set = {col: f"incoming.{col}" for col in update_cols}
        update_condition = " OR ".join([f"existing.{col} <> incoming.{col}" for col in update_cols])

        insert_values = {col: f"incoming.{col}" for col in set(primary_keys + update_cols)}

        target_delta_table.alias("existing")\
        .merge(source_df.alias("incoming"),merge_condition)\
        .whenMatchedUpdate(condition=update_condition, set={**update_set,"Updated_Date":F.current_timestamp()})\
        .whenNotMatchedInsert(values={**insert_values,"Created_Date":F.current_timestamp(),"Updated_Date":F.current_timestamp(),"JobrunID": F.date_format(F.current_timestamp(), "yyyyMMdd").cast("int")}).execute()
    
    def insert_only(self, source_df, target_table_name, primary_keys):
        """Inserts new records into the Silver table."""
        delta_table = DeltaTable.forName(self.spark, target_table_name)
        
        # Get the existing records from the Delta table
        existing_df = delta_table.toDF()
        
        # Ensure that the primary keys are correctly set for the join
        join_condition = [source_df[col] == existing_df[col] for col in primary_keys]

        # Filter out records that already exist in the Silver table
        new_records_df = source_df.alias("source").join(
            existing_df.alias("target"),
            join_condition,
            "left_anti"  # Keep only new records
        )
        
        # Insert the new records into the Silver table
        if new_records_df.count() > 0:
            new_records_df = new_records_df.withColumn("Start_Date", current_timestamp()).withColumn("JobrunID", F.date_format(F.current_timestamp(), "yyyyMMdd").cast("int"))
            new_records_df.write.mode("append").saveAsTable(target_table_name)
            
            print(f"Inserted {new_records_df.count()} new records into Silver table '{target_table_name}'.")
        else:
            print("No new records to insert into Silver table.")


    def load_silver(self):
        for ftype in self.file_type:
            if ftype == "excel":
                for table, target_table_name in self.tableName.items():
                    source_df = self.spark.read.format("delta").table(f"bronze.{table}")
                    primary_keys = self.business_keys.get(table)
                    if self.load_strategy == 'SCD-Type2':
                        if not self.spark.catalog.tableExists(target_table_name):
                            print(target_table_name)
                            #source_df = spark.read.format("delta").table(f"bronze.{table}")

                            source_df = source_df.withColumn("Start_Date", F.current_date()).withColumn("End_Date", F.lit('null')).withColumn("IsCurrent", F.lit(1)).withColumn("JobrunID", F.date_format(F.current_timestamp(), "yyyyMMdd").cast("int"))
                            source_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(target_table_name)
                        else:    
                            self.scd_type2(source_df, target_table_name, primary_keys)
                    elif self.load_strategy == 'Upsert':
                        if not self.spark.catalog.tableExists(target_table_name):
                            source_df = self.spark.read.format("delta").table(f"bronze.{table}").withColumn("Created_Date", F.current_timestamp()).withColumn("Updated_Date", F.current_timestamp()).withColumn("JobrunID", F.date_format(F.current_timestamp(), "yyyyMMdd").cast("int"))
                            source_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table_name)
                        else:    
                            self.upsert(source_df, target_table_name, primary_keys)
                    elif self.load_strategy == 'Insertsonly':
                        if not self.spark.catalog.tableExists(target_table_name):
                            source_df = self.spark.read.format("delta").table(f"bronze.{table}").withColumn("Created_Date", F.current_timestamp()).withColumn("JobrunID", F.date_format(F.current_timestamp(), "yyyyMMdd").cast("int"))
                            source_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table_name)
                        else:    
                            self.insert_only(source_df, target_table_name, primary_keys)
            elif ftype == "csv":
                for (val, i, p , l) in zip(self.read_path,self.bronze_table,self.primary_keys,self.load_strategy):
                #Get Bronze Table Name
                    self.path_data(val)
                    self.check_if_most_columns_are_string()
                    self.path_data(val)
                    self.cast_dtpes()
                    self.replaceUnwantedChars()
                    source_df = self.load_path_df
                    target_table_name = "silver." + i
                    if l == 'SCD-Type2':
                        if not self.spark.catalog.tableExists(target_table_name):
                            print(target_table_name)
                            source_df = source_df.withColumn("Start_Date", F.current_date()).withColumn("End_Date", F.lit('null')).withColumn("IsCurrent", F.lit(1)).withColumn("JobrunID", F.date_format(F.current_timestamp(), "yyyyMMdd").cast("int"))
                            source_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(target_table_name)
                        else:  
                            self.scd_type2(source_df, target_table_name, p)
                    elif l == 'Upsert':
                        if not self.spark.catalog.tableExists(target_table_name):
                            source_df = source_df.withColumn("Created_Date", F.current_timestamp()).withColumn("Updated_Date", F.current_timestamp()).withColumn("JobrunID", F.date_format(F.current_timestamp(), "yyyyMMdd").cast("int"))
                            source_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table_name)
                        else:    
                                self.upsert(source_df, target_table_name, p)
                    elif l == 'Insertsonly':
                        if not self.spark.catalog.tableExists(target_table_name):
                            source_df = source_df.withColumn("Start_Date", F.current_timestamp()).withColumn("JobrunID", F.date_format(F.current_timestamp(), "yyyyMMdd").cast("int"))
                            source_df.write.format("delta").mode("overwrite").saveAsTable(target_table_name)
                        else:    
                            self.insert_only(source_df, target_table_name, p)
        
    def check_if_most_columns_are_string(self):
        # Count how many columns are of StringType
        string_columns = [field.name for field in self.load_path_df.schema.fields if isinstance(field.dataType, StringType)]
        total_columns = len(self.load_path_df.columns)

        # Check if more than half of the columns are of StringType
        if len(string_columns) > total_columns / 2:
            self.inferSchema = True 
            return self.inferSchema
        else:
            self.inferSchema = False
            return self.inferSchema

    def replaceUnwantedChars(self):
        # Step 1: Clean unwanted characters from the `name` and `fruit` columns
        for column in self.load_path_df.columns:
            if isinstance(self.load_path_df.schema[column].dataType, StringType):
            # Remove unwanted characters from strings (non-alphanumeric characters except space)
                df = self.load_path_df.withColumn(column, F.regexp_replace(F.col(column), r'[^a-zA-Z0-9\s]', ''))
        return self.load_path_df




    def cast_dtpes(self):
        for column in self.load_path_df.columns:
            column_type = self.load_path_df.schema[column].dataType
            # Check if the column is of StringType
            if isinstance(column_type, StringType):
                # Check if the column is an array in string form and cast it to ArrayType
                if self.load_path_df.filter(F.col(column).rlike(r'^\[.*\]$')).count() > 0:
                    self.load_path_df = self.load_path_df.withColumn(column,F.explode(F.from_json(F.col(column), ArrayType(StringType()))))

                # Check if it's a valid integer string and cast it to IntegerType
                elif self.load_path_df.filter(F.col(column).rlike(r'^[+-]?\d+$')).count() > 0:
                    df = self.load_path_df.withColumn(
                        column,
                        F.when(
                            # Check for Short range: ShortType range is from -2^15 to 2^15-1
                            (F.col(column).cast("long") >= -2**15) & (F.col(column).cast("long") <= 2**15 - 1),
                            F.col(column).cast(ShortType())  # Cast to ShortType if in range
                        )
                        .otherwise(
                            F.when(
                                # Check for Integer range: IntegerType range is from -2^31 to 2^31-1
                                (F.col(column).cast("long") >= -2**31) & (F.col(column).cast("long") <= 2**31 - 1),
                                F.col(column).cast(IntegerType())  # Cast to IntegerType if in range
                            )
                            .otherwise(F.col(column).cast(LongType()))  # Cast to LongType for larger numbers
                        )
                    )
                # Check if it's a valid float or double string and cast it to DoubleType
                elif self.load_path_df.filter(F.col(column).rlike(r'^[+-]?\d*\.\d+$')).count() > 0:
                    self.load_path_df = self.load_path_df.withColumn(column, F.col(column).cast(DoubleType()))  # Cast to DoubleType for float/double values

                elif self.load_path_df.filter(F.col(column).rlike(r'^\d{4}-\d{2}-\d{2}$')).count() > 0:
                    self.load_path_df = self.load_path_df.withColumn(
                        column, 
                        F.when(
                        F.col(column).rlike(r'^\d{4}-\d{2}-\d{2}$'),  # Check if the value matches the date pattern
                        F.to_date(F.col(column), "yyyy-MM-dd")  # Cast to DateType
                        ).otherwise(F.lit(None)))
                    
                elif self.load_path_df.filter(F.col(column).rlike(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$')).count() > 0:
                    self.load_path_df = self.load_path_df.withColumn(
                        column, 
                        F.when(
                            F.col(column).rlike(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'),  # Check if the value matches timestamp pattern
                            F.to_timestamp(F.col(column), "yyyy-MM-dd HH:mm:ss")  # Cast to TimestampType
                            )
                        .otherwise(F.lit(None))  # If it's not a valid date or timestamp, set it to null
                        )


                # Check if the column is an array in string form and cast it to MapType
                elif self.load_path_df.filter(F.col(column).rlike(r'^\{.*\}$')).count() > 0:
                    # Step 1: Parse the column into a MapType
                    self.load_path_df = self.load_path_df.withColumn(
                        column, 
                        F.from_json(F.col(column), MapType(StringType(), StringType()))
                        )

                    # Step 2: Dynamically extract keys from the map and create new columns
                    # Exploding the map to get the distinct keys
                    keys = self.load_path_df.select(F.explode(F.map_keys(F.col(column)))).distinct().rdd.flatMap(lambda x: x).collect()

                    # Step 3: For each key, create a new column in the DataFrame
                    for key in keys:
                        self.load_path_df = self.load_path_df.withColumn(
                                key, 
                                (F.col(column)).getItem(F.lit(key))
                            )

                    # Optionally, drop the original map column if you no longer need it
                    self.load_path_df = self.load_path_df.drop(column)

                # Otherwise, keep it as StringType (non-numeric strings will remain as-is)
                else:
                    self.load_path_df = self.load_path_df.withColumn(column, F.col(column))
        return self.load_path_df     
        
        
    

                    