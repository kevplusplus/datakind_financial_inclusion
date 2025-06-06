import os
from pyspark.sql import types
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def load_data(file_path, spark, schema):
    """
    Load financial inclusion data from various file formats
    Supports CSV, Excel, and potentially others based on file extension
    """
    file_extension = os.path.splitext(file_path)[1].lower()
    
    try:
        if file_extension == '.csv':
            df = spark.read \
                .option("header", "true") \
                .schema(schema) \
                .csv(file_path)
        elif file_extension == '.json':
            df = spark.read \
                .option("header", "true") \
                .schema(schema) \
                .json(file_path)
        elif file_extension == '.parquet':
            df = spark.read \
                .option("header", "true") \
                .schema(schema) \
                .parquet(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_extension}")
        
        print(f"Successfully loaded data with {df.count()} rows and {df.columns} columns")
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        return None


def main():

    print(os.environ["JAVA_HOME"])


    #postgres info for jdbc connector
    user = os.environ["PG_USER"]
    password = os.environ["PG_PASSWORD"]
    host = os.environ["DB_HOST"]
    port = os.environ["DB_PORT"]
    database = os.environ["DB_NAME"]
    # table = os.environ["TABLE_NAME"]

    properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver"
    }

    url = f'jdbc:postgresql://{host}:{port}/{database}'

    #file path to data
    current_dir = os.getcwd()
    file_name = "data/vietnam_financial_inclusion_data.csv"

    file_path = os.path.join(current_dir,file_name)

    #manually defined schema
    raw_schema = types.StructType([
        types.StructField("Country Name", types.StringType(), False),
        types.StructField("Series Name", types.StringType(), False),
        types.StructField("2017", types.DoubleType(), True),
        types.StructField("2022", types.DoubleType(), True),
    ])


    spark = SparkSession.builder \
        .appName('Financial Inclusion Ingestion') \
        .getOrCreate()
    
    raw_df = load_data(file_path, spark, raw_schema)

    if raw_df is None:
        print("No file loaded.")
        return

    #trimming last 5 rows since they do not contribute to the data
    row_count = raw_df.count()
    
    if row_count <= 5:
        print("Not enough rows to trim, skipping.")
    else:
        raw_df = raw_df.orderBy(F.desc("Series Name")).limit(raw_df.count()-5)
        raw_df = raw_df.orderBy(F.asc("Series Name"))

    #drop country column since we are only working with one country
    raw_df = raw_df.drop("Country Name")

    #removing missing data
    raw_df = raw_df.dropna(thresh=1, subset=["2017","2022"])

    #write to local postgres database
    try:
        raw_df.write.jdbc(url=url, table="stg.vietnam_financial_inclusion", mode="overwrite", properties=properties)
        print("✅ Data written to PostgreSQL.")
    except Exception as e:
        print(f"❌ Failed to write to PostgreSQL: {e}")
    
    spark.stop()

    
if __name__ == "__main__":
    main()

    