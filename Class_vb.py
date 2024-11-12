# Databricks notebook source
# MAGIC %run "/Workspace/Users/yashmhatre01@gmail.com/VaibhavSir/Pytest"

# COMMAND ----------

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit,lower,col
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException
import yaml  

class CSVToTable:

    def __init__(self, table_name, path, load_strategy, primary_keys):
        self.table_name = table_name
        self.path = path
        self.load_strategy = load_strategy
        self.primary_keys = primary_keys
        self.silver_table_name = "silver." + self.table_name
        self.spark = SparkSession.builder.appName("CSVToTable").getOrCreate()

    @classmethod
    def from_yaml(cls, yaml_file_path):
        with open(yaml_file_path, 'r') as file:
            config = yaml.safe_load(file)
            return cls(
                table_name=config['table_name'],
                path=config['path'],
                load_strategy=config['load_strategy'],
                primary_keys=config['primary_keys']
            )

    def read_csv(self):
        """Reads the CSV file and returns a DataFrame."""
        try:
            df = self.spark.read.csv(self.path, header=True, inferSchema=True)
            print(f"CSV file '{self.path}' read successfully.")
            return df
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            return None

    def write_to_bronze(self, df: DataFrame):
        validator= DataValidator()
        validator.validate_dataframe(df,self.primary_keys)
        bronze_table_name = "bronze." + self.table_name

        # Check if the table exists
        if spark.catalog.tableExists(bronze_table_name):
            #If the table exists, run the schema validation
            validator.runschema(bronze_table_name, df)
        else:
            print(f"Table {bronze_table_name} does not exist.")

        """Writes the DataFrame to the Bronze table."""
        try:
            df.write.mode("overwrite").saveAsTable(bronze_table_name)
            print(f"Data written to Bronze table '{bronze_table_name}' successfully.")
        except AnalysisException as e:
            print(f"Error writing to Bronze table: {e}")

    def write_silver_table(self, df: DataFrame):
        load_strategy = self.load_strategy.lower()
        if load_strategy == "upsert":
            silver_df = df.withColumn("start_date", current_timestamp().cast("timestamp")) \
                        .withColumn("updated_date", lit(None).cast("timestamp"))
        elif load_strategy == "insertsonly":
            silver_df = df.withColumn("Created_date", current_timestamp().cast("timestamp"))
        else:
            print("Not Available")
        # Check if the Silver table exists
        df_exists = self.table_exists(self.silver_table_name)
           
        silver_table_name = "silver." + self.table_name 

        if df_exists is not None and df_exists.columns:
            print(f"Silver table '{self.silver_table_name}' exists.")
            validator= DataValidator()
            #validator.runschema(self.silver_table_name, df)
            
            if load_strategy == "upsert":
                validator = DataValidator()
                validator.DuplicateTable( df ,silver_table_name)
                print("Performing upsert.")
                self.upsert(silver_table_name, silver_df, self.primary_keys)  
                print(f"Data loaded into Silver table '{self.silver_table_name}' successfully.")
            elif load_strategy == "insertsonly":
                print("Performing insert only.")
                self.insert_only(df)  
                print(f"New records inserted into Silver table '{self.silver_table_name}' successfully.")
        else:
            print(f"Silver table '{self.silver_table_name}' does not exist. Creating it now.")
            silver_df.write.mode("overwrite").saveAsTable(self.silver_table_name)
            print(f"Silver table '{self.silver_table_name}' created successfully.")

    def insert_only(self, df):
        """Inserts new records into the Silver table."""
        delta_table = DeltaTable.forName(self.spark, self.silver_table_name)
        
        # Get the existing records from the Delta table
        existing_df = delta_table.toDF()
        
        # Ensure that the primary keys are correctly set for the join
        join_condition = [df[col] == existing_df[col] for col in self.primary_keys]

        # Filter out records that already exist in the Silver table
        new_records_df = df.alias("source").join(
            existing_df.alias("target"),
            join_condition,
            "left_anti"  # Keep only new records
        )
        
        # Insert the new records into the Silver table
        if new_records_df.count() > 0:
            new_records_df = new_records_df.withColumn("Created_date", current_timestamp())
            new_records_df.write.mode("append").saveAsTable(self.silver_table_name)
            
            print(f"Inserted {new_records_df.count()} new records into Silver table '{self.silver_table_name}'.")
        else:
            print("No new records to insert into Silver table.")

    def upsert(self, silver_table_name, silver_df, primary_key_columns):
        delta_table = DeltaTable.forName(self.spark, silver_table_name)

    # Get the schema of the Delta table
        delta_table_columns = [field.name for field in delta_table.toDF().schema.fields]

    # Determine the columns for update and insert
        update_cols = [col for col in delta_table_columns if col not in primary_key_columns]  
        insert_cols = [col for col in delta_table_columns if col not in primary_key_columns]

    # Prepare dynamic set for whenMatchedUpdate
        update_set = {col: f"source.{col}" for col in update_cols if col != "start_date"}
        update_set["updated_date"] = "current_timestamp()"

    # Prepare dynamic values for whenNotMatchedInsert
        insert_values = {pk: f"source.{pk}" for pk in primary_key_columns}  
        insert_values.update({col: f"source.{col}" for col in insert_cols})
        insert_values["start_date"] = "current_timestamp()"
        insert_values["updated_date"] = "current_timestamp()"

    # Perform the merge operation
        delta_table.alias("target").merge(
            silver_df.alias("source"),
            " AND ".join([f"target.{pk} = source.{pk}" for pk in primary_key_columns])  
        ).whenMatchedUpdate(
        set=update_set
        ).whenNotMatchedInsert(
            values=insert_values
        ).execute()

    def table_exists(self, silver_table_name):
        query = f"SELECT * FROM {silver_table_name}"
        try:
            df = self.spark.sql(query)
            return df
        except AnalysisException:
            print(f"Table '{silver_table_name}' does not exist.")
            from pyspark.sql.types import StructType
            empty_schema = StructType([])
            return self.spark.createDataFrame([], schema=empty_schema)

    def __repr__(self):
        return (f"SourceTableConfig(table_name='{self.table_name}', "
                f"path='{self.path}', load_strategy='{self.load_strategy}', "
                f"primary_keys={self.primary_keys})")

    def run(self):
        # Read the CSV file
        df = self.read_csv()

        # Check if the DataFrame was created successfully
        if df is not None:
            # Write to the Bronze table
            self.write_to_bronze(df)

            # Write to the Silver table, handling the logic based on load strategy
            self.write_silver_table(df)
        else:
            print("Data load strategy failed.")

