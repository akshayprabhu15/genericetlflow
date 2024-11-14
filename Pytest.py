from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col
import pytest
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession
import os
from datetime import date
import datetime,calendar
from etl_pipeline import Etl

class DataValidator(Etl):
    def __init__(self, spark: SparkSession):
        # Initialize the parent class (Etl) constructor
        super().__init__(spark)
        
        # You can add other custom attributes if necessary
        self.load_path_df = None  # Initialize as None
        self.current = str(date.today())
        self.dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.year = datetime.datetime.today().year
        self.month= calendar.month_name[datetime.datetime.today().month]
        self.t_name = None
        
    def load_config(self, config_file_path: str):
        # Call the parent class load_config method
        return super().load_config(config_file_path)
    
    def validate_dataframe(self, df: DataFrame, primarykeys: list):
        """Check if required columns exist, check for null values, and check for unique primary key values for a composite key."""
    
        # Check if each primary key column exists
        for primarykey in primarykeys:
            if primarykey not in df.columns:
                print(f"Validation failed: Primary key column '{primarykey}' is missing from the DataFrame.")
                raise ValueError(f"Primary key column '{primarykey}' is missing from the DataFrame.")
            else:
                print(f"Primary key column '{primarykey}' found in DataFrame.")
    
            # Check for null values in the primary key column
            null_count = df.filter(df[primarykey].isNull()).count()
            if null_count > 0:
                print(f"Validation failed: Primary key column '{primarykey}' contains {null_count} null values.")
                raise ValueError(f"Primary key column '{primarykey}' contains null values.")
            else:
                print(f"No null values found in primary key column '{primarykey}'.")
        
        # Check for uniqueness of the composite primary key (combination of the primary key columns)
        distinct_count = df.select(primarykeys).distinct().count()
        total_count = df.count()
        if distinct_count != total_count:
            print(f"Validation failed: Composite primary key contains {total_count - distinct_count} duplicate rows.")
            raise ValueError(f"Composite primary key contains duplicate rows.")
        else:
            print(f"Composite primary key contains unique rows.")
        
        # If all checks pass
        print("DataFrame validation passed successfully.")
        return 

    
    def DuplicateTable(self, df: DataFrame, s_df,s_table_name) -> None:
        
        # Load the existing silver table DataFrame
        #s_df = self.spark.table(s_table_name)

        metadata_cols = ["Start_Date","End_Date","Created_Date", "Updated_Date","JobrunID","IsCurrent"]
        dup_check_cols = [x for x in s_df.columns if x not in set(metadata_cols)]
        dup_df = df.join(s_df, dup_check_cols, 'inner').select(df.columns)
        
        
        # Debugging: Print the count of rows in both DataFrames
        print(f"Row count in new DataFrame: {dup_df.count()}")
        print(f"Row count in existing DataFrame: {s_df.count()}")
        source_df = df.subtract(dup_df)
        # If the number of matching rows equals the number of rows in df, print and raise an error
        if source_df.count() == 0 : 
            print("The new DataFrame contains duplicate entries from the {s_table_name} table.")
            raise ValueError("The new DataFrame contains duplicate entries from the {s_table_name} table.")
        else:
            print("The new DataFrame does not contain duplicate entries from the existing table.")

        return True  # Return True if they are not identical
    
    def get_table_schema(self,table_name):
        """Fetch schema (column names and types) from an existing table using `describe`."""
        schema_df = spark.sql(f"DESCRIBE {table_name}")
        table_schema ={row['col_name']: row['data_type'] for row in schema_df.collect()}
        return table_schema

    def get_df_schema(self,df):
        """Get schema (column names and types) from a DataFrame."""
        dfschema = dict(df.dtypes)
        return dfschema

    def test_schema_match(self,table_schema, df_schema):
       
        # Check that each column in the DataFrame matches the corresponding column in the table
        for col_name, df_dtype in df_schema.items():
            if col_name in table_schema:
                # Assert the data type matches if column names match
                assert table_schema[col_name] == df_dtype, (
                    f"Data type mismatch for column '{col_name}'. "
                    f"Table data type: {table_schema[col_name]}, DataFrame data type: {df_dtype}"
                )
            else:
                # Raise an assertion error if a column in the DataFrame is not found in the table schema
                pytest.fail(f"Column '{col_name}' not found in the table schema.")
    
        print(f"Schema verification successful: All columns and data types match for table .")
    
    def runschema(self,table_name, df):
        validator = DataValidator()
        tabl_sch= validator.get_table_schema(table_name)
        df_sch = validator.get_df_schema(df)
        validator.test_schema_match(tabl_sch,df_sch)

    def validate_rawdata(self,config_file_path):
        self.file_type,self.bronze_table,self.read_path,self.load_strategy,self.primary_keys = self.load_config(config_file_path)
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
            #Check if its csv
            elif ftype == "csv":
                print("Loading CSV File")
                for (tb , val ,p) in zip(self.bronze_table,self.read_path, self.primary_keys):
                    self.path_data(val)
                    for pk in p:
                        self.validate_dataframe(self.load_path_df,p)
    
    def table_exists(self, silver_table_name):
        query = f"SELECT * FROM {silver_table_name}"
        try:
            df = self.spark.sql(query)
            return True
        except AnalysisException:
            print(f"Table '{silver_table_name}' does not exist.")
            return False    
    
    def validate_duplicate(self,config_file_path):
        self.file_type,self.bronze_table,self.read_path,self.load_strategy,self.primary_keys = self.load_config(config_file_path)
        
        for (ftype,tb , val) in zip(self.file_type,self.bronze_table,self.read_path):
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
            #Check if its csv
            elif ftype == "csv":
                print("Loading CSV File")
                s_table_name = f"silver.{tb}"
                if self.spark.catalog.tableExists(s_table_name):
                    s_df = self.spark.table(s_table_name)
                    self.path_data(val)
                    try:
                        self.DuplicateTable(self.load_path_df,s_df,s_table_name)
                    except ValueError:
                        continue
                    self.relicatebtable(tb)

    def checkdir(self):
        
        if not os.path.exists(f'/Volumes/yash/volumes/backup-data/RAW/{self.t_name}/{self.year}/{self.month}/{self.current}'):
            raise AssertionError("No Path Found Creating one")
        else:
            print("Path Exist")

    def writedir(self,b_df):
        try:
            self.checkdir()
        except AssertionError:
            os.makedirs(f'/Volumes/yash/volumes/backup-data/RAW/{self.t_name}/{self.year}/{self.month}/{self.current}')

    def relicatebtable(self,tb):
        self.t_name = tb
        b_table = f"bronze.{tb}"
        b_df = self.spark.table(b_table)
        # check and write dir with current date if not available
        self.writedir(b_df)
        #replicate the bronze table
        b_df.write.option("header", "true") \
            .mode("append") \
            .csv(f'/Volumes/yash/volumes/backup-data/RAW/{self.t_name}/{self.year}/{self.month}/{self.current}/')
