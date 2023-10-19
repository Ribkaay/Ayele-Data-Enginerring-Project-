"""This project is a Data Extraction, Transformation, load (ETL) proccess implemented using  with Python and PySpark. 
It proccess two datasets: Loan Application and a Credit Card dataset. This project use various technologies and libraries Python (Pandas, advanced modules, e.g., Matplotlib), SQL,
Apache Spark (Spark Core, Spark SQL), and Python Visualization and Analytics libraries.   

Author: Ribka Ayele
Date: 10/14/2023"""

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

#Create a Spark session
spark = SparkSession.builder.appName("Credit Card System").getOrCreate()

#Extract cdw_sapp_custmer, cdw_sapp_branch, and cdw_sapp_creditJSON files
#Read the Json data form my local computer 
df_cdw_sapp_custmer = spark.read.json("cdw_sapp_custmer.json")
df_cdw_sapp_custmer.printSchema() #show df_cdw_sapp_custmer schema
print(df_cdw_sapp_custmer.show()) 

#extract cdw_sapp_branch Json data
df_cdw_sapp_branch = spark.read.json("cdw_sapp_branch.json")
df_cdw_sapp_branch.printSchema()
print(df_cdw_sapp_branch.show(5))


#extract cdw_sapp_credit Json data
df_cdw_sapp_credit = spark.read.json("cdw_sapp_credit.json")
df_cdw_sapp_credit.printSchema()
print(df_cdw_sapp_credit.show(5))

"""  Functional Requirements - Load Credit Card Database (SQL)
Req-1.1 Data Extraction and Transformation with Python and  PySpark
Transform cdw_sapp_custme file based on the mapping document according to the 
specifications found in the mapping document; 
- """

# Map df_cdw_sapp_custmer data
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
                                                                    substring(col("CUST_PHONE"),4,3).cast(StringType()))
                                                    ).withColumn("CUST_ZIP", lpad(col("CUST_ZIP"),5,"0"))


                                                      
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
                                  ).withColumn("CUST_ZIP", col("CUST_ZIP").cast(StringType())    
                                  ).withColumn("CUST_PHONE", col("CUST_PHONE").cast(StringType())
                                  ).withColumn("CUST_EMAIL", col("CUST_EMAIL").cast(StringType()) 
                                  ).withColumn("LAST_UPDATED", col("LAST_UPDATED").cast(TimestampType()))

  # # Drop Apt No and street number. FULL_STREET_ADDRESS includes both
  # cdw_sapp_custmer=cdw_sapp_custmer.drop("STREET_NAME","APT_NO")                       
print("cdw_sapp_custmer after mapping")                         
cdw_sapp_custmer.show(8) 


""" Transform cdw_sapp_branch data frame based on the mapping document"""

df_cdw_sapp_branch = df_cdw_sapp_branch.select("BRANCH_CODE","BRANCH_NAME","BRANCH_STREET","BRANCH_CITY","BRANCH_STATE",
                                                 "BRANCH_ZIP","BRANCH_PHONE","LAST_UPDATED")

  #IF BRANCH_ZIP  value is null load default (99999) value 
  #Change the format of phone number to(xxx)xxx-xxx format
  #Make sure BRANCH_ZIP  code 5 number

df_cdw_sapp_branch = df_cdw_sapp_branch.withColumn("BRANCH_ZIP", lpad(col("BRANCH_ZIP"),5,"0"))

df_cdw_sapp_branch = df_cdw_sapp_branch.withColumn("BRANCH_ZIP", when(col("BRANCH_ZIP").isNull(),'99999').otherwise(col("BRANCH_ZIP"))
                                ).withColumn("BRANCH_PHONE",concat(lit('('),substring(col("BRANCH_PHONE"),1,3),lit(')'),
                                                                  substring(col("BRANCH_PHONE"),4,3),lit('-'),
                                                                  substring(col("BRANCH_PHONE"),7,4)))                              

df_cdw_sapp_branch.show(5)

#Conver into targeted data type except Branch code, zip code and last update convert all to stringtype
 
cdw_sapp_branch = df_cdw_sapp_branch.withColumn("BRANCH_CODE", col("BRANCH_CODE").cast(IntegerType())
                                ).withColumn('BRANCH_NAME', col("BRANCH_NAME").cast(StringType())
                                  ).withColumn('BRANCH_STREET', col("BRANCH_STREET").cast(StringType())
                                  ).withColumn('BRANCH_CITY', col("BRANCH_CITY").cast(StringType()) 
                                  ).withColumn('BRANCH_STATE', col("BRANCH_STATE").cast(StringType())
                                  ).withColumn('BRANCH_ZIP', col("BRANCH_ZIP").cast(StringType())
                                  ).withColumn('BRANCH_PHONE', col("BRANCH_PHONE").cast(StringType())
                                  ).withColumn("LAST_UPDATED", col("LAST_UPDATED").cast(TimestampType()))  
cdw_sapp_branch.show(8)
cdw_sapp_branch.printSchema()

"""Transform cdw_sapp_credit data"""

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
cdw_sapp_credit.show(8)
  
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

db_connection() 


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

cdw_sapp_custmer = load_to_mysql(cdw_sapp_custmer,"cdw_sapp_custmer")
cdw_sapp_branch = load_to_mysql(cdw_sapp_branch,"cdw_sapp_branch")
cdw_sapp_credit = load_to_mysql(cdw_sapp_credit, "cdw_sapp_credit")

def read_to_my_sql(table_name,spark):
  # spark = SparkSession.builder.appName("CreditCardSystem").getOrCreate()
  
    # Conncatnate the table name with the database name
    full_table_name = "creditcard_capstone." + table_name

    table = spark.read.format("jdbc").options(driver = "com.mysql.cj.jdbc.Driver",
            user =  secret.mysql_username,\
            password = secret.mysql_password,\
            url = "jdbc:mysql://localhost:3306/creditcard_capstone",\
            dbtable = full_table_name).load() #Ex:creditcard_capstone.cdw_sapp_custmer
    table.show(3)
    return table
cdw_sapp_custmer = read_to_my_sql("cdw_sapp_custmer", spark)
cdw_sapp_branch = read_to_my_sql("cdw_sapp_branch",spark )
cdw_sapp_credit = read_to_my_sql( "cdw_sapp_credit",spark)

cdw_sapp_custmer.createOrReplaceTempView("cdw_sapp_custmer")
cdw_sapp_branch .createOrReplaceTempView("cdw_sapp_branch")
cdw_sapp_credit.createOrReplaceTempView("cdw_sapp_credit")

"""Application Front-End  Transaction Details"""

"""2. Functional Requirements - Application Front-End
# # Once data is loaded into the database, we need a front-end (console) to see/display data. For
# # that, create a console-based Python program to satisfy System Requirements 2 (2.1 and 2.2)"""

# Get zip_code, Month, year from the user
#Example Zip_code:23112  Month:03  year:2018  Z: 52804 M: 08 y 2018

 #Create a Spark session
spark = SparkSession.builder.appName("Credit Card System").getOrCreate()

zip_code = input("Please enter the Zip code (ex:23112): ")
Month = input("Please enter the month (ex:03): ")
Year = input("Please enter the year (ex:2018): ")
                             
#Conver TIMEID to Month and Year by split or using substring slicing 
credit_Tempview = cdw_sapp_credit.withColumn("YEAR", substring(col("TIMEID"), 1,4).cast("INT")
                                              ).withColumn("MONTH", (substring(col("TIMEID"), 5,2).cast("INT"))
                                              ).withColumn("DAY", (substring(col("TIMEID"), 7,2).cast("INT")))

credit_Tempview.createOrReplaceTempView("credit_Tempview")
# Used to display the transactions made by customers living in agiven zip code for a given month and year.
#  Order by day in descending order.

customer_transaction = f""" SELECT c.FIRST_NAME,c.LAST_NAME,c.CREDIT_CARD_NO,c.FULL_STREET_ADDRESS,c.CUST_ZIP, cc.CUST_CC_NO,
       cc.TRANSACTION_TYPE, cc.TRANSACTION_VALUE FROM cdw_sapp_custmer c  INNER JOIN credit_Tempview  cc  ON c.CREDIT_CARD_NO = cc.CUST_CC_NO
      wHERE c.CUST_ZIP = '{zip_code}' AND cc.MONTH = '{Month}' AND cc.YEAR = '{Year}'ORDER BY cc.DAY DESC"""
 
result = spark.sql(customer_transaction)
result.show(5)

""" Used to display the number and total values of transactions for a given type."""
type = input("Please enter a transactions type(ex: Gas): ")

#use count to get total number of transaction

spark_sql= f""" SELECT COUNT(*) AS TRANSATION_TYPE, '{type}' FROM cdw_sapp_credit WHERE cdw_sapp_credit.TRANSACTION_TYPE = '{type}'"""

result = spark.sql(spark_sql)    
result.show() 


""" Used to display the total number and total values of transactions for branches in a given state."""

State = input("Please enter the state(ex:WA): ")

spark_sql =f""" SELECT '{State}' AS STATE, COUNT(c.TRANSACTION_ID) AS TOTAL_TRANSACTION, 
SUM(c.TRANSACTION_VALUE) AS TOTAL_TRANSACTION_VALUE 
FROM cdw_sapp_branch b JOIN cdw_sapp_credit c  ON b.BRANCH_CODE = c.BRANCH_CODE 
WHERE b.BRANCH_STATE = '{State}' """


result = spark.sql(spark_sql)    
result.show() 

"""Customer Details
 
 
 2.2 Customer Details Module
# Functional Requirements 2.2
# 1) Used to check the existing account details of a customer.
# 2) Used to modify the existing account details of a customer.
# 3) Used to generate a monthly bill for a credit card number for a given month and year.
# 4) Used to display the transactions made by a customer between
# two dates. Order by year, month, and day in descending order 
"""

#1) Used to check the existing account details of a customer.

SSN = input("Please enter the 9 digit of customer SSN(123456100): ")

#Check customer existing account
spark_Sql = f""" SELECT * FROM cdw_sapp_custmer WHERE '{SSN}' = SSN """
result = spark.sql(spark_Sql)
result.show()


# 3) Used to display the total number and total values of transactions
# for branches in a given state
#spark = SparkSession.builder.appName("Credit Card System").getOrCreate()
spark_sql = "select * from cdw_sapp_credit where CREDIT_CARD_NO = '4210653349028689'"
result = spark.sql(spark_sql)
result.show()

#3) Used to generate a monthly bill for a credit card number for a given month and year.
#4210653349028689
                 
credits_Card_number = input("Please enter the credit card number(ex:4210653349028689): ")
Month = int(input("Please enter the month (ex:4): "))
Year = int(input("Please enter the year (ex: 2018): "))

monthly_bill = credit_Tempview.filter((credit_Tempview['MONTH']== Month) & (credit_Tempview['YEAR'] == Year) & (credit_Tempview['CREDIT_CARD_NO'] == credits_Card_number))
monthly_bill.show()

#4) Used to display the transactions made by a customer between
#two dates. Order by year, month, and day in descending order

spark = SparkSession.builder.appName("CreditCardSystem").getOrCreate()
customer_SSN = input("Please enter the SSN(ex:123459988): ")
start_date = input("Please enter the start Date YYYYMMD (ex:20180412): ")
end_date = input("Please enter the end Date YYYYMMD (ex:20181012): ")

#query transaction for customer between two dates
query = f""" SELECT * FROM credit_Tempview WHERE CUST_SSN = '{customer_SSN}' AND 
TIMEID BETWEEN '{start_date}' AND '{end_date}' ORDER BY YEAR DESC,MONTH DESC, DAY DESC"""
result = spark.sql(query)
result.show()

"""Data Analysis and Visualization for Load Credit Card Database (SQL)
    Functional Requirements 3.1 Find and plot which transaction type has the highest transaction count.
    Functional Requirements 3.2Find and plot which state has a high number of customers.
    Functional Requirements 3.3 Find and plot the sum of all transactions for the top 10 customers,
    and which customer has the highest transaction amount."""


#Functional Requirements 3.1Find and plot which transaction type has 
#the highest transaction count

def highest_customers_state(): #To access it call this function

 

    conn = mysql.connector.connect(database='creditcard_capstone',
                                user = secret.mysql_username,
                               password = secret.mysql_password)
    cursor = conn.cursor() 
    try:
     
        # selct the highst transaction and state from cdw_sapp_custmer
        #data
            
        query = """SELECT CUST_STATE, COUNT(*) AS CUSTOMER_COUNT
                FROM cdw_sapp_custmer GROUP BY CUST_STATE 
                ORDER BY CUSTOMER_COUNT DESC """
        cursor.execute(query)
        
        data = cursor.fetchall()  #Fectch the result into dataframe
        df = pd.DataFrame(data, columns = ['CUST_STATE','CUSTOMER_COUNT'])
        print(df)
        plt.figure(figsize=(8,7))

        plt.bar(df['CUST_STATE'], df['CUSTOMER_COUNT'])
        plt.title("State with the Highest Number of Customers ")
        plt.xlabel("State")
        plt.ylabel("Customer Count")
        #Add a grid lines
        # plt.grild(True, linestyle = '--', alpha =0.7)
        plt.show()
        cursor.close()

    except Error as e:
        print(e)
    finally:
      conn.close()

highest_customers_state()

# Find and plot which transaction type has the highest transaction count.

def highest_transaction_type(): #To access it call this function

    conn = mysql.connector.connect(database='creditcard_capstone',
                               user = secret.mysql_username,
                               password = secret.mysql_password)
    cursor = conn.cursor() 
 
    try:
        conn = mysql.connector.connect(database='creditcard_capstone',
                                user = secret.mysql_username,
                               password = secret.mysql_password)
        cursor = conn.cursor() 
     
        # selct the highst transaction and state from cdw_sapp_custmer
        #data
            
        query = """SELECT TRANSACTION_TYPE,COUNT(*) AS TRANSACTION_COUNT
                FROM  cdw_sapp_credit GROUP BY TRANSACTION_TYPE 
                ORDER BY TRANSACTION_COUNT DESC """
        cursor.execute(query)
        
        data = cursor.fetchall()  #Fectch the result into dataframe
        df = pd.DataFrame(data, columns = ['TRANSACTION_TYPE','TRANSACTION_COUNT'])
        print(df)
        plt.figure(figsize=(10,6))

        plt.bar(df['TRANSACTION_TYPE'], df['TRANSACTION_COUNT'])
        plt.title("Transaction Type with the Highest Transaction Count")
        plt.xlabel("TRANSACTION_TYPE")
        plt.ylabel("TRANSACTION_COUNT'")
        #Add a grid lines
        #.grild(True, linestyle = '--')
        plt.show()
        cursor.close()

    except Error as e:
        print(e)
    finally:
      conn.close()



highest_transaction_type()

#Functional Requirements 3.3 Find and plot the sum of all transactions for the
# top 10 customers,and which customer has the highest transaction amount.

def top10_transaction():
 
 try: 
        conn = mysql.connector.connect(database='creditcard_capstone',
                               user = secret.mysql_username,
                               password = secret.mysql_password)
        cursor = conn.cursor() 

        # selct the total transaction from cdw_sapp_credit  
        quary = """SELECT CUST_SSN, SUM(TRANSACTION_VALUE) AS TOTAL 
                FROM cdw_sapp_credit
                GROUP BY CUST_SSN ORDER BY TOTAL DESC  LIMIT 10"""

        cursor.execute(quary)

        top_10 = cursor.fetchall()

        df = pd.DataFrame(top_10, columns = ['CUST_SSN','TOTAL'])
        print(df)
        df.plot.barh(x = 'CUST_SSN', y = 'TOTAL')
        plt.title("Top 10 Customers Transaction Amount")
        plt.xlabel("Transaction Amount")
        plt.ylabel("Customer SSN")
        plt.figure(figsize=(10,7))
        plt.show()
        
 except Exception as e:
        print("error")
        conn.rollback()
 finally:
        cursor.close()
        conn.close()

top10_transaction()



"""  LOAN Application Dataset
Req-4 Access to Loan API Endpoint
Functional Requirements 4.1
Create a Python program to GET (consume) data from the above API endpoint for the loan application dataset.
Functional Requirements 4.2 Find the status code of the above API endpoint. Hint: status code could be 200, 400, 404, 401.
Functional Requirements 4.3 Once Python reads data from the API, utilize PySpark to load data into
RDBMS (SQL). The table name should be CDW-SAPP_loan_application in the database. Note: Use the “creditcard_capstone” database."""


def Loan_Application_dataset():
    spark = SparkSession.builder.appName('Loan Application dataset').getOrCreate()

    """ 4. Functional Requirements - LOAN Application Dataset  
     On this part of project I used  LOAN Application dataset. First https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json 
    request acces from a REST API by sending an HTTP request and processing the response. After got the respond load it to pyspark and load it on database. 
    Finally create o analyze and visualize the data."""

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


    table_name = "`CDW_SAPP_loan_application`"

    data_loan_df.write.format("jdbc") \
        .mode("overwrite") \
        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
        .option("dbtable", table_name) \
        .option("user", secret.mysql_username) \
        .option("password",secret.mysql_password) \
        .save()

    data_loan_df.show()
    spark.stop()

Loan_Application_dataset()

#Find the percentage of rejection for married male applicants


def reject_percentage_for_married_male():

 
    conn = mysql.connector.connect(database='creditcard_capstone',
                                user = secret.mysql_username,
                               password = secret.mysql_password)
    cursor = conn.cursor() 
    try:
        query = """SELECT Application_Status FROM CDW_SAPP_loan_application
               WHERE Application_Status = 'N' AND Gender = 'Male' AND Married = 'Yes'; """
        
        cursor.execute (query ) 
        data = cursor.fetchall ()

        # Create a DataFrame from the query result 
        df= pd.DataFrame(data, columns = ['Application_Status']) 
      # Count the number of rejected married male applicants 
        total_rejected = len(df )  
        
        #SQL query to select total married male applicants
        query = """SELECT Application_Status 
         FROM CDW_SAPP_loan_application
         WHERE Gender = 'Male' AND Married = 'Yes'; """
        
        cursor.execute (query ) 
        data= cursor.fetchall () # Create a DataFrame from the query result 
        df = pd.DataFrame(data, columns = ['Application_Status']) # Count the total number of married male applicants 
        total_married_males = len(df) 
        print(df)
      # Calculate the rejection percentage
        rejection_percentage = (total_rejected / total_married_males) * 100
        approval_percentage = 100 - rejection_percentage

        labels = ['Rejection', 'Approval'] 
        percentages = [rejection_percentage, approval_percentage ] 
        colors = ['yellow', 'green']
      
        plt.pie( percentages, labels = labels, colors = colors, autopct = '%1.1f%%' )
        plt.title ('Loan Application Rejection percentage for Married Males') 
        plt.show ()
        
    except Exception as e:
            print("error")
            conn.rollback()
    finally:
            cursor.close()
            conn.close()

reject_percentage_for_married_male()

#Find and plot the top three months with the largest volume of transaction data.

def Top3_month_transaction():

 
    conn = mysql.connector.connect(database='creditcard_capstone',
                                user = secret.mysql_username,
                               password = secret.mysql_password)
    cursor = conn.cursor() 
    try:
        query = """SELECT SUBSTRING(TIMEID, 1, 6) AS MONTH, SUM(TRANSACTION_VALUE) AS TOT_TRANSACTION_VALUE
        FROM cdw_sapp_credit GROUP BY MONTH ORDER BY TOT_TRANSACTION_VALUE DESC LIMIT 3"""

        cursor.execute (query ) 
        data = cursor.fetchall ()
        #Fectch the result into dataframe
        df = pd.DataFrame(data, columns = ['MONTHE','TOT_TRANSACTION_VALUE'])
        print(df)
        plt.figure(figsize=(10,6))

        plt.bar(df['MONTHE'], df['TOT_TRANSACTION_VALUE'])
        plt.title("Top Three Month by Transaction Volume")
        plt.xlabel("MONTH")
        plt.ylabel("TOTAL TRANSACTION_VALUE'")
        #Add a grid lines
        #.grild(True, linestyle = '--')
        plt.show()
        cursor.close()

    except Error as e:
        print(e)
    finally:
      conn.close()
        
Top3_month_transaction()



