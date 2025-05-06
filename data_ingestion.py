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
    table = os.environ["TABLE_NAME"]

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
    schema = types.StructType([
        types.StructField('Country Name', types.StringType(), True),
        types.StructField('Series Name', types.StringType(), True),
        types.StructField('2017', types.DecimalType(5,2), True),
        types.StructField('2022', types.DecimalType(5,2), True),
    ])

    spark = SparkSession.builder \
        .appName('Financial Inclusion Ingestion') \
        .getOrCreate()
    
    # not necessary since copied jar driver directly to spark/jars which automatically load the jar
        # .config("spark.jars","/app/drivers/postgresql-42.7.5.jar") \
        # .config("spark.driver.extraClassPath", "/app/drivers/postgresql-42.7.5.jar") \
        # .config("spark.executor.extraClassPath", "/app/drivers/postgresql-42.7.5.jar") \
        
    
    #   not necessary since using docker image spark-submit
    #     .master("spark://localhost:7077") \

    df = load_data(file_path, spark, schema)

    if df is None:
        print("None")
        return
    
    row_count = df.count()

    #removing last 5 rows since they do not contribute to the data
    if row_count <= 5:
        print("Not enough rows to trim, skipping.")
    else:
        df = df.orderBy(F.desc("Series Name")).limit(df.count()-5)
        df = df.orderBy(F.asc("Series Name"))

    #removing country column since we are only working with one country
    df = df.drop("Country Name")

    #renaming column
    df = (df.withColumnRenamed("Series Name", "survey_question")
           .withColumnRenamed("2017", "vietnam_2017") 
           .withColumnRenamed("2022", "vietnam_2022"))

    #removing missing data
    df = df.dropna(thresh=1, subset=["vietnam_2017","vietnam_2022"])

    df.show(5)

    #write to local postgres database
    try:
        df.write.jdbc(url=url, table=table, mode="overwrite", properties=properties)
        print("✅ Data written to PostgreSQL.")
    except Exception as e:
        print(f"❌ Failed to write to PostgreSQL: {e}")
    
    spark.stop()

    # print(f"Current data with {df.count()} rows and {df.columns} columns")
    # df.show()
    # df.printSchema()

    # df.createOrReplaceTempView('temp_view')

    # query = """
    # SELECT survey_question, vietnam_2017, vietnam_2022
    # FROM temp_view
    # WHERE vietnam_2017 IS NOT NULL AND vietnam_2022 IS NULL
    # """

    # spark.sql(query).show()

    
if __name__ == "__main__":
    main()

    