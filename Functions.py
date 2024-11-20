from pyspark.sql.types import ArrayType, StructType, StructField, StringType,IntegerType,DoubleType,BooleanType,TimestampType,MapType,ShortType,LongType
from pyspark.sql import functions as F

def cast_to_short_int_long(df):
    for column in df.columns:
            column_type = df.schema[column].dataType
            # Check if the column is of StringType
            if isinstance(column_type, StringType):
                if df.filter(F.col(column).rlike(r'^[+-]?\d+$')).count() > 0:
                    df = df.withColumn(column,F.when(
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
                elif df.filter(F.col(column).rlike(r'^[+-]?\d*\.\d+$')).count() > 0:
                    df = df.withColumn(column, F.col(column).cast(DoubleType()))  # Cast to DoubleType for float/double values
    return df

def cast_string_array_map(df):
    for column in df.columns:
            column_type = df.schema[column].dataType
            # Check if the column is of StringType
            if isinstance(column_type, StringType):
                # Check if the column is an array in string form and cast it to ArrayType and explode it
                if df.filter(F.col(column).rlike(r'^\[.*\]$')).count() > 0:
                    df = df.withColumn(column,F.explode(F.from_json(F.col(column), ArrayType(StringType()))))
                    # Check if the column is an array in string form and inside array if its MapType and cast it to MapeType 
                    if df.filter(F.col(column).rlike(r'^\[.*\]$')) and df.filter(F.col(column).rlike(r'^\{.*\}$')).count() > 0:
                        df = df.withColumn(column,F.from_json(F.col(column), MapType(StringType(), StringType())))
                    # Check if the column is an Map in string form and cast it to MapType    
                    elif df.filter(F.col(column).rlike(r'^\{.*\}$')).count() > 0:
                        df = df.withColumn(column,F.from_json(F.col(column), MapType(StringType(), StringType())))
    return df

def explode_map(df):
    for column in df.columns:
        column_type = df.schema[column].dataType
        if isinstance(column_type, MapType):
            # Step 1: Dynamically extract keys from the map and create new columns
            #Exploding the map to get the distinct keys
            keys = df.select(F.explode(F.map_keys(F.col(column)))).distinct().rdd.flatMap(lambda x: x).collect()

            # Step 3: For each key, create a new column in the DataFrame
            for key in keys:
                df = df.withColumn(key,(F.col(column)).getItem(F.lit(key)))

            # Optionally, drop the original map column if you no longer need it
            df = df.drop(column)
    return df

def cast_date_type(df):
    for column in df.columns:
            column_type = df.schema[column].dataType
            # Check if the column is of StringType
            if isinstance(column_type, StringType):
                if df.filter(F.col(column).rlike(r'^\d{4}-\d{2}-\d{2}$')).count() > 0:
                    df = df.withColumn(
                        column, 
                        F.when(
                        F.col(column).rlike(r'^\d{4}-\d{2}-\d{2}$'),  # Check if the value matches the date pattern
                        F.to_date(F.col(column), "yyyy-MM-dd")  # Cast to DateType
                        ).otherwise(F.lit(None)))
    return df

def cast_timestamp_type(df):
    for column in df.columns:
            column_type = df.schema[column].dataType
            # Check if the column is of StringType
            if isinstance(column_type, StringType):
                if df.filter(F.col(column).rlike(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$')).count() > 0:
                    df = df.withColumn(
                        column, 
                        F.when(
                        F.col(column).rlike(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'),  # Check if the value matches timestamp pattern
                        F.to_timestamp(F.col(column), "yyyy-MM-dd HH:mm:ss")  # Cast to TimestampType
                        )
                        .otherwise(F.lit(None))  # If it's not a valid date or timestamp, set it to null
                        )
    return df