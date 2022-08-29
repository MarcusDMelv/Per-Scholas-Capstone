import json
import requests
from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder.appName('SparkApp').getOrCreate()


# use request to load
def get_json_data():
    data_link = 'https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json'
    result = requests.get(data_link)
    raw_data = json.loads(result.text)
    print('Current Status Code:', result.status_code)
    return raw_data


# use for later
def create_pandas_df():
    data_link = 'https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json'
    df = pd.read_json(data_link)
    df.to_csv('loan_app_data.csv', index=False)  # created json file
    return df


# just cause
def create_spark_df():
    df = spark.read.option('header', True).csv('loan_app_data.csv')
    df.printSchema()
    return df


# export to maria db
def load_data_to_sql():
    df = create_spark_df()
    df.write.format("jdbc") \
        .mode("append") \
        .option("url", "jdbc:mysql://localhost:3307/creditcard_capstone") \
        .option("dbtable", "creditcard_capstone.CDW_SAPP_loan_application") \
        .option("user", "root") \
        .option("password", "Pass1234") \
        .save()


# pandas_df will be needed for DAV
pandas_df = create_pandas_df()
