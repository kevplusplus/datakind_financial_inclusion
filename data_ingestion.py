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

    #time dimension schema
    time_dimension_schema = types.StructType([
        types.StructField("time_id", types.IntegerType(), False),
        types.StructField("year", types.IntegerType(), False)
    ])

    #indicators dimension schema
    indicators_schema = types.StructType([
        types.StructField("indicator_id", types.LongType(), False),
        types.StructField("indicator_name", types.StringType(), False)
    ])

    #response types dimension schema
    response_types_schema = types.StructType([
        types.StructField("response_type_id", types.LongType(), False),
        types.StructField("response_type_name", types.StringType(), True)  # Nullable since some indicators may not have a response type
    ])

    #demographics dimension schema
    demographics_schema = types.StructType([
        types.StructField("demographic_id", types.LongType(), False),
        types.StructField("age_category", types.StringType(), False),
        types.StructField("gender", types.StringType(), True),
        types.StructField("income_level", types.StringType(), True),
        types.StructField("geography", types.StringType(), True),
        types.StructField("employment_status", types.StringType(), True),
        types.StructField("education_level", types.StringType(), True)
    ])

    fact_schema = types.StructType([
        types.StructField("fact_id", types.LongType(), False),
        types.StructField("indicator_id", types.LongType(), False),
        types.StructField("response_type_id", types.LongType(), False),
        types.StructField("demographic_id", types.LongType(), False),
        types.StructField("time_id",types.IntegerType(), False),
        types.StructField("value", types.DoubleType(), True),
        types.StructField("measurement_description", types.StringType(), True)
    ])


    spark = SparkSession.builder \
        .appName('Financial Inclusion Ingestion') \
        .getOrCreate()
    
    raw_df = load_data(file_path, spark, raw_schema)

    if raw_df is None:
        print("No file loaded.")
        return

    #removing last 5 rows since they do not contribute to the data
    row_count = raw_df.count()
    
    if row_count <= 5:
        print("Not enough rows to trim, skipping.")
    else:
        raw_df = raw_df.orderBy(F.desc("Series Name")).limit(raw_df.count()-5)
        raw_df = raw_df.orderBy(F.asc("Series Name"))

    #removing country column since we are only working with one country
    raw_df = raw_df.drop("Country Name")

    #removing missing data
    raw_df = raw_df.dropna(thresh=1, subset=["2017","2022"])

    #unpivot data
    unpivoted_df = raw_df.select(
        "Series Name",
        F.expr("stack(2, '2017', `2017`, '2022', `2022`) as (year, value)")
    )

    #parsing from Series Name for our dimension tables
    parsed_df = unpivoted_df.withColumn(
        "indicator", F.regexp_extract(F.col("Series Name"), r"^([^:()]+)(?::|\s*\(|\s*,)", 1)
    ).withColumn(
        "response_type", F.when(
            F.col("Series Name").contains(":"), 
            F.regexp_extract(F.col("Series Name"), r":\s*([^(]+)", 1)
        ).otherwise(F.lit("no response"))
    ).withColumn(
        "age_category", F.when(
            F.col("Series Name").contains(", young"), F.lit("young")
        ).when(
            F.col("Series Name").contains(", older"), F.lit("older")
        ).otherwise(F.lit("all adults"))
    ).withColumn(
        "gender", F.when(
            F.col("Series Name").contains(", male"), F.lit("male")
        ).when(
            F.col("Series Name").contains(", female"), F.lit("female")
        ).otherwise(F.lit(None))
    ).withColumn(
        "income_level", F.when(
            F.col("Series Name").contains(", income, poorest 40%"), F.lit("poorest 40%")
        ).when(
            F.col("Series Name").contains(", income, richest 60%"), F.lit("richest 60%")
        ).otherwise(F.lit(None))
    ).withColumn(
        "geography", F.when(
            F.col("Series Name").contains(", rural"), F.lit("rural")
        ).when(
            F.col("Series Name").contains(", urban"), F.lit("urban")
        ).otherwise(F.lit(None))
    ).withColumn(
        "employment_status", F.when(
            F.col("Series Name").contains(", in labor force"), F.lit("in labor force")
        ).when(
            F.col("Series Name").contains(", out of labor force"), F.lit("out of labor force")
        ).otherwise(F.lit(None))
    ).withColumn(
        "education_level", F.when(
            F.col("Series Name").contains(", primary education or less"), F.lit("primary education or less")
        ).when(
            F.col("Series Name").contains(", secondary education or more"), F.lit("secondary education or more")
        ).otherwise(F.lit(None))
    ).withColumn(
        "measurement_description",
        F.regexp_extract(F.col("Series Name"), r"\((.*?)\)", 1)
    )

    #create dimension tables with schemas and parsed df
    time_df = parsed_df.select("year").distinct()
    time_df = time_df.withColumn("year", F.col("year").cast(types.IntegerType()))
    time_df = time_df.withColumn("time_id", F.col("year"))
    
    time_df = time_df.select("time_id", "year") #need specific order to match spark schema
    time_df = spark.createDataFrame(time_df.rdd, time_dimension_schema)

    indicators_df = parsed_df.select("indicator").distinct()
    indicators_df = indicators_df.withColumn("indicator_id", F.monotonically_increasing_id()) #creating indicator_ids
    indicators_df = indicators_df.withColumnRenamed("indicator", "indicator_name")
    
    indicators_df = indicators_df.select("indicator_id", "indicator_name")
    indicators_df = spark.createDataFrame(indicators_df.rdd, indicators_schema)

    response_types_df = parsed_df.select("response_type").distinct()
    response_types_df = response_types_df.withColumn("response_type_id", F.monotonically_increasing_id())
    response_types_df = response_types_df.withColumnRenamed("response_type", "response_type_name")
    
    response_types_df = response_types_df.select("response_type_id", "response_type_name")
    response_types_df = spark.createDataFrame(response_types_df.rdd, response_types_schema)

    demographics_df = parsed_df.select(
        "age_category", "gender", "income_level", "geography", "employment_status", "education_level"
    ).distinct()
    demographics_df = demographics_df.withColumn("demographic_id", F.monotonically_increasing_id())
    demographics_df = demographics_df.select(
        "demographic_id", "age_category", "gender", "income_level", "geography", "employment_status", "education_level"
    )
    demographics_df = spark.createDataFrame(demographics_df.rdd, demographics_schema)

    #join tables together to create fact table
    fact_df = parsed_df.join(
        indicators_df,
        on=parsed_df["indicator"] == indicators_df["indicator_name"], 
        how="left"
    ).join(
        response_types_df, 
        on=parsed_df["response_type"] == response_types_df["response_type_name"], 
        how="left"
    ).join(
        demographics_df, 
        on=[
            parsed_df["age_category"] == demographics_df["age_category"],
            parsed_df["gender"].eqNullSafe(demographics_df["gender"]),
            parsed_df["income_level"].eqNullSafe(demographics_df["income_level"]),
            parsed_df["geography"].eqNullSafe(demographics_df["geography"]),
            parsed_df["employment_status"].eqNullSafe(demographics_df["employment_status"]),
            parsed_df["education_level"].eqNullSafe(demographics_df["education_level"])
        ],
        how="left"
    ).join(
        time_df, on="year", how="left"
    )

    
    fact_df = fact_df.select(
        F.monotonically_increasing_id().alias("fact_id"),
        "indicator_id",
        "response_type_id",
        "demographic_id",
        "time_id",
        "value",
        "measurement_description"
    )

    fact_df = spark.createDataFrame(fact_df.rdd, fact_schema)

    #write to local postgres database
    try:
        time_df.write.jdbc(url=url, table="dim.time", mode="overwrite", properties=properties)
        indicators_df.write.jdbc(url=url, table="dim.indicators", mode="overwrite", properties=properties)
        response_types_df.write.jdbc(url=url, table="dim.response_type", mode="overwrite", properties=properties)
        demographics_df.write.jdbc(url=url, table="dim.demographics", mode="overwrite", properties=properties)
        fact_df.write.jdbc(url=url, table="fact.vietnam_financial_inclusion", mode="overwrite", properties=properties)
        print("✅ Data written to PostgreSQL.")
    except Exception as e:
        print(f"❌ Failed to write to PostgreSQL: {e}")
    
    spark.stop()

    # query = """
    # SELECT survey_question, vietnam_2017, vietnam_2022
    # FROM temp_view
    # WHERE vietnam_2017 IS NOT NULL AND vietnam_2022 IS NULL
    # """

    # spark.sql(query).show()

    
if __name__ == "__main__":
    main()

    