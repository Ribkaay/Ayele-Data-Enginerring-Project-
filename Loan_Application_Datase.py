import os
import sys
import pyspark
import requests
import pandas as pd 
import random
import secret
import matplotlib.pyplot as plt
import mysql.connector
from mysql.connector import Error

from pyspark.sql.functions import when,col, substring, concat,lit,initcap, lpad,substring, lower
from pyspark.sql.types import IntegerType,StringType,TimestampType, DoubleType

from pyspark.sql import SparkSession  #import sparksession from pyspark.sql



""" 4. Functional Requirements - LOAN Application Dataset  
 On this part of project I used  LOAN Application dataset. First https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json 
 request acces from a REST API by sending an HTTP request and processing the response. After got the respond load it to pyspark and load it on database. 
 Finally create o analyze and visualize the data.
"""


def Loan_Application_dataset():
    spark = SparkSession.builder.appName('Loan Application dataset').getOrCreate()

    # Create a Python program to GET (consume) data from the above API
    # endpoint for the loan application dataset.

    url = "https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json"

    #Request the API endpoint
    response = requests.get(url)
    data_loan = response.json()  #get Json data from the response

    """Find the status code of the above API endpoint.
    Hint: status code could be 200, 400, 404, 401"""

    print(f"The request status code is: {response.status_code}" )  # print response.status_code

    """Once Python reads data from the API, utilize PySpark to load data into
    RDBMS (SQL). The table name should be CDW-SAPP_loan_application
    in the database.Note: Use the “creditcard_capstone” database."""

    #Load in to spark dataframe
    data_loan_df = spark.createDataFrame(data_loan)

    # #Define  properties to connect to database
    mysql_properties = {
        "user":  secret.mysql_username,
    "password" :  secret.mysql_password,
    "drive": "com.msql.cj.jdbc.Driver",
    }


    table_name = "`CDW-SAPP_loan_application`"

    data_loan_df.write.format("jdbc") \
        .mode("overwrite") \
        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
        .option("dbtable", table_name) \
        .option("user", secret.mysql_username) \
        .option("password",secret.mysql_password) \
        .save()

    data_loan_df.show()
    spark.stop()



def main():


   #request acces from a REST API and load the respond on database
  Loan_Application_dataset()



  
if __name__=="__main__":
  main()



