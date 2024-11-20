from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col
import pytest
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, BooleanType, ArrayType, MapType, TimestampType
from pyspark.sql import SparkSession ,Row
import os
import csv
from datetime import date,datetime
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
        self.multiline = False
        self.header = False
        self.trailing_whitespace = False
        self.leading_whitespace = True
        self.inferSchema = False
        self.result_df = None
        self.timestamp = datetime.datetime.now()
        
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
        # Load the existing silver table DataFramw
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
    
    def check_and_create_database(self):
        # Check if the 'silver' database exists
        databases = self.spark.catalog.listDatabases()
        if "silver" not in [db.name for db in databases]:
            self.spark.sql("CREATE DATABASE silver")

    def log_test_result(self, test_name, columns, status, table_name, error_message=None):

        # Define the schema for the DataFrame
        schema = StructType([
            StructField("test_name", StringType(), True),
            StructField("tested_columns", StringType(), True),
            StructField("status", StringType(), True),
            StructField("error_message", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("timestamp", TimestampType(), True),
        ])

        # Create a Row to represent the test result
        result_row = Row(
            test_name=test_name,
            tested_columns=columns,
            status=status,
            error_message=error_message,
            table_name=table_name,
            timestamp=self.timestamp
        )

        # Convert the Row to a DataFrame using the defined schema
        self.result_df = self.spark.createDataFrame([result_row], schema)

        # Append the result to the test_results table
        self.result_df.write.format("delta").mode("append").saveAsTable("silver.test_results")
        return self.result_df

    
    # Test 1: No null values Primary Key colum in specific columns
    def test_no_null_values(self, silver_df, pk, table_name):
        # Check if pk is a list
        if isinstance(pk, list):
        # Handle as a list of columns
            for column in pk:
                null_count = silver_df.filter(col(column).isNull()).count()
                if null_count > 0:
                    self.log_test_result("No Null Values", column, "Fail", table_name, f"Column {column} has {null_count} null values.")
                else:
                    self.log_test_result("No Null Values", column, "Pass", table_name)
        else:
            # Handle as a single column
            null_count = silver_df.filter(col(pk).isNull()).count()
            if null_count > 0:
                self.log_test_result("No Null Values", pk, "Fail", table_name, f"Column {pk} has {null_count} null values.")
            else:
                self.log_test_result("No Null Values", pk, "Pass", table_name)

    # Test 2: Column values are unique 
    def test_unique(self, silver_df, column, table_name):
        # Check if column is a list
        if isinstance(column, list):
            for col_name in column:
                duplicate_count = silver_df.groupBy(col_name).count().filter(col("count") > 1).count()
                if duplicate_count > 0:
                    self.log_test_result("Unique", col_name, "Fail", table_name, f"Found {duplicate_count} duplicate {col_name} values.")
                else:
                    self.log_test_result("Unique", col_name, "Pass", table_name)
        else:
            duplicate_count = silver_df.groupBy(column).count().filter(col("count") > 1).count()
            if duplicate_count > 0:
                self.log_test_result("Unique", column, "Fail", table_name, f"Found {duplicate_count} duplicate {column} values.")
            else:
                self.log_test_result("Unique", column, "Pass", table_name)

    # Test 3: No empty strings in text columns
    def test_no_empty_strings(self,silver_df, column, table_name):
        empty_count = silver_df.filter((col(column) == "") | col(column).isNull()).count()
        if empty_count > 0:
            self.log_test_result("No Empty Strings", column, "Fail", table_name, f"Column {column} has {empty_count} empty strings or nulls.")
        else:
            self.log_test_result("No Empty Strings", column, "Pass", table_name)

    # Test 4: No duplicate rows in specific key columns
    def test_no_duplicate_entries(self, silver_df, key_columns, table_name):
        # Check if 'key_columns' is a list
        if isinstance(key_columns, list):
            # Handle as a list of key columns
            duplicate_count = silver_df.groupBy(*key_columns).count().filter(col("count") > 1).count()
            if duplicate_count > 0:
                self.log_test_result("No Duplicate Entries", ", ".join(key_columns), "Fail", table_name, f"Found {duplicate_count} duplicate rows for keys {key_columns}.")
            else:
                self.log_test_result("No Duplicate Entries", ", ".join(key_columns), "Pass", table_name)
        else:
            # Handle as a single key column
            duplicate_count = silver_df.groupBy(key_columns).count().filter(col("count") > 1).count()
            if duplicate_count > 0:
                self.log_test_result("No Duplicate Entries", key_columns, "Fail", table_name, f"Found {duplicate_count} duplicate rows for key {key_columns}.")
            else:
                self.log_test_result("No Duplicate Entries", key_columns, "Pass", table_name)


    # Test 5: Date consistency in date columns (StartDate > EndDate)
    def test_date_consistency(self,silver_df, table_name, load_strategy):
        if load_strategy == "Upsert":
            invalid_date_count = silver_df.filter((col("Created_Date") > col("Updated_date"))).count() 
            if invalid_date_count > 0:
                self.log_test_result("Date Consistency", "Created_Date,Updated_date", "Fail", table_name, f"Found {invalid_date_count} rows where Created_Date > Updated_date")
            else:
                self.log_test_result("Date Consistency", "Created_Date,Updated_date", "Pass", table_name)
        elif load_strategy == "SCD-Type2":
            invalid_date_count = silver_df.filter(col("Start_Date") > col("End_Date")).count() 
            if invalid_date_count > 0:
                self.log_test_result("Date Consistency", "Start_Date, End_date", "Fail", table_name, f"Found {invalid_date_count} rows where Created_Date > Updated_date")
            else:
                self.log_test_result("Date Consistency", "Created_Date,Updated_date", "Pass", table_name)
        elif load_strategy == "Insertsonly":
            self.log_test_result("Date Consistency", "Start_Date", "Skipped", table_name, f"No Multiple Dates Found")
        
    def dataqualitychecks(self):
        # Ensure the 'silver.test_results' table exists
        self.check_and_create_database()  # Ensure the silver database exists
        if not self.spark._jsparkSession.catalog().tableExists("silver.test_results"):
            self.spark.sql("""
                CREATE TABLE IF NOT EXISTS silver.test_results (
                    test_name STRING,
                    tested_columns STRING,
                    status STRING,
                    error_message STRING,
                    table_name STRING,
                    timestamp TIMESTAMP
                ) USING DELTA
            """)
        for ( i, p,l) in zip(self.bronze_table,self.primary_keys,self.load_strategy):
            table_name = "silver." + i
            silver_df = self.spark.table(table_name)
            self.test_no_null_values(silver_df, p, table_name)
            self.test_unique(silver_df,p, table_name)
            for column in silver_df.columns:
               if isinstance(silver_df.schema[column].dataType, StringType):
                        self.test_no_empty_strings(silver_df, column, table_name)
            self.test_no_duplicate_entries(silver_df, p, table_name)
            self.test_date_consistency(silver_df, table_name,l)

    
