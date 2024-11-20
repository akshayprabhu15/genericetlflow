from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col,xxhash64
import pytest
from pyspark.sql import SparkSession
import os 
from datetime import date
import calendar
import datetime
import sys

class DataValidator:
    def __init__(self):
        self.current = str(date.today())
        self.t_name =
        self.dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.year = datetime.datetime.today().year
        self.month= calendar.month_name[datetime.datetime.today().month]


    def validate_dataframe(self, df: DataFrame, primarykeys):
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

    
    
    def DuplicateTable(self, df: DataFrame, s_table_name: str) -> None:
        
        # Load the existing silver table DataFrame
        s_df = spark.table(s_table_name)

        metadata_cols = ["created_dt", "updated_dt","JobrunID"]
        dup_check_cols = [x for x in s_df.columns if x not in set(metadata_cols)]
        dup_df = df.join(s_df, dup_check_cols, 'inner').select(df.columns)
        
        
        # Debugging: Print the count of rows in both DataFrames
        print(f"Row count in new DataFrame: {dup_df.count()}")
        print(f"Row count in existing DataFrame: {s_df.count()}")
        source_df = df.subtract(dup_df)
        # If the number of matching rows equals the number of rows in df, print and raise an error
        if source_df.count() == 0 : 
            print("Data is Duplicate of "+ s_table_name +".")
            raise ValueError("The new DataFrame contains duplicate entries from the  table.")
        else:
            print("The new DataFrame does not contain duplicate entries from the existing table.")

        return True  # Return True if they are not identical
    
    def get_table_schema(self,table_name):
        """Fetch schema (column names and types) from an existing table using `describe`."""
        metadata_columns = ["created_dt", "updated_dt","JobrunID"]
        schema_df = spark.sql(f"DESCRIBE {table_name}")
        table_schema ={row['col_name']: row['data_type'] for row in schema_df.collect() if row['col_name'] not in metadata_columns}
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
        b_table = "bronze." + tb
        b_df = spark.table(b_table)
        # check and write dir with current date if not available
        self.writedir(b_df)
        #replicate the bronze table
        b_df.write.option("header", "true") \
            .mode("append") \
            .csv(f'/Volumes/yash/volumes/backup-data/RAW/{tb}/{self.year}/{self.month}/{self.current}/')


