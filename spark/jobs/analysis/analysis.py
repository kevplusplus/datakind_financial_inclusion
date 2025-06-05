import os
from pyspark.sql import SparkSession


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

    spark = SparkSession.builder \
        .appName('Financial Inclusion Analysis') \
        .getOrCreate()
    

    time_df = spark.read.jdbc(url=url, table="dim.time", properties=properties)
    indicators_df = spark.read.jdbc(url=url, table="dim.indicators", properties=properties)
    response_types_df = spark.read.jdbc(url=url, table="dim.response_type", properties=properties)
    demographics_df = spark.read.jdbc(url=url, table="dim.demographics", properties=properties)
    fact_df = spark.read.jdbc(url=url, table="fact.vietnam_financial_inclusion", properties=properties)

    denormalized_view = (
        fact_df
        .join(indicators_df, on="indicator_id", how="left")
        .join(response_types_df, on="response_type_id", how="left")
        .join(demographics_df, on="demographic_id", how="left")
        .join(time_df, on="time_id", how="left")
    )

    denormalized_view.write.jdbc(url=url,table="public.denormalized_indicators_view",mode="overwrite",properties=properties)

if __name__ == "__main__":
    main()

    