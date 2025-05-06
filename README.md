# 📊 Financial Inclusion Data Ingestion

A Spark-based data ingestion pipeline that reads raw financial inclusion data from CSV format and writes it into a PostgreSQL database.

## ✨ Features

- Reads CSV, JSON, and Parquet files using Apache Spark
- Cleans and transforms financial data for Vietnam
- Writes cleaned data into a local PostgreSQL database using JDBC
- Dockerized for easy setup and reproducibility

## 🛠️ Technologies

- Pyspark
- Apache Spark (bitnami/spark image)
- PostgreSQL
- Docker + Docker Compose

## 🚀 Quickstart

1. **Configure Environment Variables**

   Rename the provided `dotenv` file to `.env`

   Then update the variables inside to match your local environment (e.g., PostgreSQL credentials and host configuration).

2. **Navigate to the Project Directory**
   
   Make sure you're in the root directory of the project:

   `cd /path/to/project`

3. **Start the Application**
   Use Docker Compose to build and run the containers:

   `docker-compose up --build`

This will:
- Start the PostgreSQL service
- Build the Spark container
- Automatically run the data ingestion script

Don't forget to run `docker-compose down` to stop all running containers.

If you want to remove local postgres data you can delete `financial_inclusion_vietnam data` folder.
