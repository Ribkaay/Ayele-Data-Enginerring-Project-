'''This project is a Data Extraction, Transformation, load (ETL) proccess implemented using  with Python and PySpark. 
It proccess two datasets: Loan Application and a Credit Card dataset. This project use various technologies and libraries Python (Pandas, advanced modules, e.g., Matplotlib), SQL,
Apache Spark (Spark Core, Spark SQL), and Python Visualization and Analytics libraries.   

Author: Ribka Ayele
Date: 10/14/2023
'''
import os
import sys
import pyspark
import pandas as pd 
import random
import secret
import matplotlib.pyplot as plt
import mysql.connector
from mysql.connector import Error



from pyspark.sql.functions import when,col, substring, concat,lit,initcap, lpad,substring, lower
from pyspark.sql.types import IntegerType,StringType,TimestampType, DoubleType

from pyspark.sql import SparkSession  #import sparksession from pyspark.sql

def create_spark_session(app_name):
  #Create a Spark session
  return (SparkSession.builder.appName(app_name).getOrCreate())


#Extract cdw_sapp_custmer, cdw_sapp_branch, and cdw_sapp_creditJSON files
def extract_cdw_sapp_custmer(spark):
#Read the Json data form my local computer 
  df_cdw_sapp_custmer = spark.read.json("cdw_sapp_custmer.json")
  # df_cdw_sapp_custmer.printSchema() #show df_cdw_sapp_custmer schema
  print(df_cdw_sapp_custmer.dtypes) 

  #return etrcted data frame
  return df_cdw_sapp_custmer

#extract cdw_sapp_branch Json file 
def extract_cdw_sapp_branch (spark):

  df_cdw_sapp_branch = spark.read.json("cdw_sapp_branch.json")
  df_cdw_sapp_branch.printSchema()
  print(df_cdw_sapp_branch.show(5))

 # return df_cdw_sapp_branch
  return  df_cdw_sapp_branch

#extract cdw_sapp_credit Json file 
def extract_cdw_sapp_credit(spark):
  df_cdw_sapp_credit = spark.read.json("cdw_sapp_credit.json")
  df_cdw_sapp_credit.printSchema()
  print(df_cdw_sapp_credit.show(5))
  return df_cdw_sapp_credit  #return extract file
  
"""  Functional Requirements - Load Credit Card Database (SQL)
Req-1.1 Data Extraction and Transformation with Python and  PySpark
Transform cdw_sapp_custme file based on the mapping document according to the 
specifications found in the mapping document; 
- """
def transform_cdw_sapp_custmer (df_cdw_sapp_custmer):
  
  #Phone number missed area code so that  generate random area code
  random_area_code = (random.randint(206, 789))

  #covert area code to string  and amking sure 3 digit long
  random_area_code = f"{random_area_code:03d}"

  #Apply Transformation First & last name to title case, middle name lower case 
  #phone number add area code and in (xxx)xxx-xxx format
  cdw_sapp_custmer= df_cdw_sapp_custmer.withColumn("FIRST_NAME" , initcap(col("FIRST_NAME"))  #tile case
                                                    ).withColumn("LAST_NAME" , initcap(col("LAST_NAME"))
                                                    ).withColumn("MIDDLE_NAME" , lower(col("MIDDLE_NAME"))
                                                    ).withColumn("FULL_STREET_ADDRESS" , concat(col("STREET_NAME"), lit(","), ("APT_NO"))
                                                    ).withColumn("CUST_PHONE",concat(lit('('),lit(random_area_code),lit(')'),substring(col("CUST_PHONE"),1,3),lit(')'),
                                                                    substring(col("CUST_PHONE"),4,3).cast(StringType())))
                                                      
  cdw_sapp_custmer.show(5)



  cdw_sapp_custmer = cdw_sapp_custmer.select("SSN","FIRST_NAME","MIDDLE_NAME","LAST_NAME","CREDIT_CARD_NO",
                          "FULL_STREET_ADDRESS", "CUST_CITY","CUST_STATE","CUST_COUNTRY",
                          "CUST_ZIP", "CUST_PHONE","CUST_EMAIL", "LAST_UPDATED")
  

  #convert  all colomn to VARCAR except SSN,CUST_ZIP, APT_NO (IntegerType),LAST_UPDATED TimestampType()
  cdw_sapp_custmer=cdw_sapp_custmer.withColumn("SSN", col("SSN").cast(IntegerType())
                                  ).withColumn("FIRST_NAME", col("FIRST_NAME").cast(StringType())
                                  ).withColumn("MIDDLE_NAME", col("MIDDLE_NAME").cast(StringType())
                                  ).withColumn("LAST_NAME", col("LAST_NAME").cast(StringType())            
                                  ).withColumn("Credit_card_no", col("CREDIT_CARD_NO").cast(StringType())
                                  ).withColumn("FULL_STREET_ADDRESS", col("FULL_STREET_ADDRESS").cast(StringType())
                                  ).withColumn("CUST_CITY", col("CUST_CITY").cast(StringType()) 
                                  ).withColumn("CUST_STATE", col("CUST_STATE").cast(StringType())
                                  ).withColumn("CUST_COUNTRY", col("CUST_COUNTRY").cast(StringType()) 
                                  ).withColumn("CUST_ZIP", col("CUST_ZIP").cast(IntegerType())    
                                  ).withColumn("CUST_PHONE", col("CUST_PHONE").cast(StringType())
                                  ).withColumn("CUST_EMAIL", col("CUST_EMAIL").cast(StringType()) 
                                  ).withColumn("LAST_UPDATED", col("LAST_UPDATED").cast(TimestampType()))

  # # Drop Apt No and street number. FULL_STREET_ADDRESS includes both
  # cdw_sapp_custmer=cdw_sapp_custmer.drop("STREET_NAME","APT_NO")                       
                          
  cdw_sapp_custmer.show(5) 

  return cdw_sapp_custmer 

""" Transform cdw_sapp_branch data frame based on the mapping document"""
def transform_cdw_sapp_branch(df_cdw_sapp_branch):

  df_cdw_sapp_branch = df_cdw_sapp_branch.select("BRANCH_CODE","BRANCH_NAME","BRANCH_STREET","BRANCH_CITY","BRANCH_STATE",
                                                 "BRANCH_ZIP","BRANCH_PHONE","LAST_UPDATED")

  #IF BRANCH_ZIP  value is null load default (99999) value 
  #Change the format of phone number to(xxx)xxx-xxx format
  #Make sure BRANCH_ZIP  code 5 number

  df_cdw_sapp_branch = df_cdw_sapp_branch.withColumn("BRANCH_ZIP", lpad(col("BRANCH_ZIP"),5,"0"))
 
  df_cdw_sapp_branch = df_cdw_sapp_branch.withColumn("BRANCH_ZIP", when(col("BRANCH_ZIP").isNull(),'99999').otherwise(col("BRANCH_ZIP"))
                                  ).withColumn("BRANCH_PHONE",concat(lit('('),substring(col("BRANCH_PHONE"),1,3),lit(')'),
                                                                    substring(col("BRANCH_PHONE"),4,3),lit('-'),
                                                                    substring(col("BRANCH_PHONE"),7,4))
                                 )

  df_cdw_sapp_branch.show()

  #Conver into targeted data type except Branch code, zip code and last update convert all to stringtype
 
  cdw_sapp_branch = df_cdw_sapp_branch.withColumn("BRANCH_CODE", col("BRANCH_CODE").cast(IntegerType())
                                ).withColumn('BRANCH_NAME', col("BRANCH_NAME").cast(StringType())
                                  ).withColumn('BRANCH_STREET', col("BRANCH_STREET").cast(StringType())
                                  ).withColumn('BRANCH_CITY', col("BRANCH_CITY").cast(StringType()) 
                                  ).withColumn('BRANCH_STATE', col("BRANCH_STATE").cast(StringType())
                                  ).withColumn('BRANCH_ZIP', col("BRANCH_ZIP").cast(IntegerType())
                                  ).withColumn('BRANCH_PHONE', col("BRANCH_PHONE").cast(StringType())
                                  ).withColumn("LAST_UPDATED", col("LAST_UPDATED").cast(TimestampType()))  
  cdw_sapp_branch.show()
  cdw_sapp_branch.printSchema()
  return cdw_sapp_branch

"""Extract cdw_sapp_credit data frame"""

def transform_cdw_sapp_credit(df_cdw_sapp_credit):

  #use lpad function for  month and day because some of them put as one degit
  # concat Year, Month and Day into a TIMEID (YYYYMMDD)
  cdw_sapp_credit = df_cdw_sapp_credit.withColumn("TIMEID",concat(col("YEAR"),lpad(col("MONTH"),2,"0"),lpad(col("DAY"),2,"0")))
  cdw_sapp_credit.show(5)

  cdw_sapp_credit= cdw_sapp_credit.select("CREDIT_CARD_NO", "TIMEID","CUST_SSN","BRANCH_CODE","TRANSACTION_TYPE",
                                          "TRANSACTION_VALUE","TRANSACTION_ID")

  #Change CREDIT_CARD_NO column name to CUST_CC_NO and cast it VARCHAr
  cdw_sapp_credit = cdw_sapp_credit.withColumn("CUST_CC_NO", col("CREDIT_CARD_NO").cast(StringType())
                            ).withColumn("TIMEID", col("TIMEID").cast(StringType())
                            ).withColumn("CUST_SSN", col("CUST_SSN").cast(IntegerType())
                            ).withColumn("BRANCH_CODE", col("BRANCH_CODE").cast(IntegerType())
                            ).withColumn("TRANSACTION_TYPE", col("TRANSACTION_TYPE").cast(StringType())
                            ).withColumn("TRANSACTION_VALUE", col("TRANSACTION_VALUE").cast(DoubleType())
                            ).withColumn("TRANSACTION_ID", col("TRANSACTION_ID").cast(IntegerType()))
  cdw_sapp_credit.show()
  return cdw_sapp_credit

"""Req-1.2 Data loading into Database
Function Requirement 1.2 Once PySpark reads data from JSON files, and then utilizes Python,
PySpark, and Python modules to load data into RDBMS(SQL), perform thefollowing:
a) Create a Database in SQL(MySQL), named “creditcard_capstone.”
b) Create a Python and Pyspark Program to load/write the “Credit
Card System Data” into RDBMS(creditcard_capstone).
Tables should be created by the following names in RDBMS: CDW_SAPP_BRANCH,CDW_SAPP_CREDIT_CARD, CDW_SAPP_CUSTOMER"""

def db_connection():
   
    #spark = SparkSession.builder.master("local[*]").appName("creditcard_capstone").getOrCreate()
    conn = None
   
    try:
        conn = mysql.connector.connect(database='creditcard_capstone',
                                             user = secret.mysql_username,
                                             password = secret.mysql_password)
        #  user=secret.mysql_password,
        #                                      password=secret.mysql_password)
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("CREATE DATABASE IF NOT EXISTS creditcard_capstone")
             #Connect to MySQL database 
            print('Connected to MySQL database')

    except Error as e:
        print(e)

    finally:
        if conn is not None and conn.is_connected():
            conn.close()

def load_to_mysql(dataframe,table_name):
  #write a pyspark dataframe to MySQL table
 table = dataframe.write.format("jdbc") \
  .mode("append") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
  .option("dbtable", table_name) \
  .option("user", secret.mysql_username) \
  .option("password",secret.mysql_password) \
  .save()
 return table 



   


def main():

  spark = create_spark_session('Credit Card System')
  #Extract JSON 
  # df_cdw_sapp_custmer = extract_cdw_sapp_custmer(spark)
  df_cdw_sapp_branch = extract_cdw_sapp_branch (spark)
  # df_cdw_sapp_credit= extract_cdw_sapp_credit(spark) 

  #Transform to dataframe 
  # cdw_sapp_custmer = transform_cdw_sapp_custmer (df_cdw_sapp_custmer)
  cdw_sapp_branch= transform_cdw_sapp_branch(df_cdw_sapp_branch)
  # cdw_sapp_credit = transform_cdw_sapp_credit(df_cdw_sapp_credit)

  #Load to Database

  db_connection()
  
  # cdw_sapp_custmer = load_to_mysql(cdw_sapp_custmer,"cdw_sapp_custmer")
  cdw_sapp_branch = load_to_mysql(cdw_sapp_branch,"cdw_sapp_branch" )
  # cdw_sapp_credit = load_to_mysql(cdw_sapp_credit, "cdw_sapp_credit")
if __name__=="__main__":
  main()
  