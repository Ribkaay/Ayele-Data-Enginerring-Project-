'''This project is a Data Extraction, Transformation, load (ETL) proccess implemented using  with Python and PySpark. 
It proccess two datasets: Loan Application dat and a Credit Card dataset. This project use various technologies and libraries Python (Pandas, advanced modules, e.g., Matplotlib), SQL,
Apache Spark (Spark Core, Spark SQL), and Python Visualization and Analytics libraries.   

Author: Ribka Ayele
Date: 10/14/2023
'''
import pyspark
import pandas as pd 
import random
import Secret
import matplotlib.pyplot as plt

from pyspark.sql.functions import when,col, substring, concat,lit,initcap, lpad,substring, lower
from pyspark.sql.types import IntegerType,StringType,TimestampType, DoubleType

from pyspark.sql import SparkSession  #import sparksession from pyspark.sql


#Create a Spark session
spark = SparkSession.builder.appName('CreditCardSystem').getOrCreate()




# def extract_from_json():
#Read the Json data form my local computer 
df_cdw_sapp_branch = spark.read.json("cdw_sapp_branch.json")
df_cdw_sapp_credit = spark.read.json("cdw_sapp_credit.json")
df_cdw_sapp_custmer = spark.read.json("cdw_sapp_custmer.json")

#Show the schema of the Json data
df_cdw_sapp_branch.printSchema()
df_cdw_sapp_credit.printSchema()
df_cdw_sapp_custmer.printSchema()
print(df_cdw_sapp_branch.show(5))
print(df_cdw_sapp_credit.show(5))
print(df_cdw_sapp_custmer.dtypes)


#def transformation cdw_sapp_custmer()


#Phone number missed area code so that  generate random area code
random_area_code = (random.randint(206, 789))

#covert area code to string  and amking sure 3 digit long
random_area_code = f"{random_area_code:03d}"

#Apply Transformation First & last name to title case, middle name lower case 
#phone number add area code and in (xxx)xxx-xxx format
cdw_sapp_custmer=df_cdw_sapp_custmer.withColumn("FIRST_NAME" , initcap(col("FIRST_NAME"))  #tile case
                                                  ).withColumn("LAST_NAME" , initcap(col("LAST_NAME"))
                                                  ).withColumn("MIDDLE_NAME" , lower(col("MIDDLE_NAME"))
                                                  ).withColumn("FULL_STREET_ADDRESS" , concat(col("STREET_NAME"), lit(","), ("APT_NO"))
                                                  ).withColumn("CUST_PHONE",concat(lit('('),lit(random_area_code),lit(')'),substring(col("CUST_PHONE"),1,3),lit(')'),
                                                                  substring(col("CUST_PHONE"),4,3)))
                                                    
cdw_sapp_custmer.show(5)


#convert  all colomn to VARCAR except SSN,CUST_ZIP, APT_NO (IntegerType)
cdw_sapp_custmer.withColumn("SSN", col("SSN").cast(IntegerType())
                                ).withColumn("FIRST_NAME", col("FIRST_NAME").cast(StringType())
                                ).withColumn("MIDDLE_NAME", col("MIDDLE_NAME").cast(StringType())
                                ).withColumn("LAST_NAME", col("LAST_NAME").cast(StringType())            
                                ).withColumn("CREDIT_CARD_NO", col("CREDIT_CARD_NO").cast(StringType())
                                ).withColumn("FULL_STREET_ADDRESS", col("FULL_STREET_ADDRESS").cast(StringType())
                                ).withColumn("CUST_CITY", col("CUST_CITY").cast(StringType()) 
                                ).withColumn("CUST_STATE", col("CUST_STATE").cast(StringType())
                                ).withColumn("CUST_COUNTRY", col("CUST_COUNTRY").cast(StringType()) 
                                ).withColumn("CUST_ZIP", col("CUST_ZIP").cast(IntegerType())    
                                ).withColumn("CUST_PHONE", col("CUST_PHONE").cast(StringType())
                                ).withColumn("CUST_EMAIL", col("CUST_EMAIL").cast(StringType()) 
                                ).withColumn("LAST_UPDATED", col("LAST_UPDATED").cast(TimestampType()))

# Drop Apt No and street number. FULL_STREET_ADDRESS includes both
cdw_sapp_custmer=cdw_sapp_custmer.drop("STREET_NAME","APT_NO")                       
                        
cdw_sapp_custmer.show(5)  


#def transformation cdw_sapp_branch()
#Transform cdw_sapp_branch data frame.IF BRANCH_ZIP  value is null load default (99999) value 
#Change the format of phone number to(xxx)xxx-xxx format
cdw_sapp_branch = df_cdw_sapp_branch.withColumn("BRANCH_ZIP", when(col("BRANCH_ZIP").isNull(),'99999').otherwise(col("BRANCH_ZIP"))
                                ).withColumn("BRANCH_PHONE",concat(lit('('),substring(col("BRANCH_PHONE"),1,3),lit(')'),
                                                                  substring(col("BRANCH_PHONE"),4,3),lit('-'),
                                                                  substring(col("BRANCH_PHONE"),7,4)))

#Conver into targeted data type except Branch code, zip code and last update convert all to stringtype
cdw_sapp_branch.withColumn("BRANCH_CODE", col("BRANCH_CODE").cast(IntegerType())
                               ).withColumn('BRANCH_NAME', col('BRANCH_NAME').cast(StringType())
                                ).withColumn('BRANCH_STREET', col('BRANCH_STREET').cast(StringType())
                                ).withColumn('BRANCH_CITY', col('BRANCH_CITY').cast(StringType()) 
                                ).withColumn('BRANCH_STATE', col('BRANCH_STATE').cast(StringType())
                                ).withColumn('BRANCH_ZIP', col('BRANCH_CODE').cast(IntegerType())
                                ).withColumn('BRANCH_PHONE', col('BRANCH_PHONE').cast(StringType())
                                ).withColumn("LAST_UPDATED", col('LAST_UPDATED').cast(TimestampType()))  


cdw_sapp_branch.show(5)



#def transformation cdw_sapp_credit()

#use lpad function for  month and day because some of them put as one degit
# concat Year, Month and Day into a TIMEID (YYYYMMDD)
cdw_sapp_credit = df_cdw_sapp_credit.withColumn("TIMEID",concat(col("YEAR"),lpad(col("MONTH"),2,"0"),lpad(col("DAY"),2,"0")))
cdw_sapp_credit.show(5)


#Change CREDIT_CARD_NO column name to CUST_CC_NO and cast it VARCHAr
cdw_sapp_credit=cdw_sapp_credit.withColumn("CUST_CC_NO", col("CREDIT_CARD_NO").cast(StringType())
                           ).withColumn("TIMEID", col("TIMEID").cast(StringType())
                           ).withColumn("CUST_SSN", col("CUST_SSN").cast(IntegerType())
                           ).withColumn("BRANCH_CODE", col("BRANCH_CODE").cast(IntegerType())
                           ).withColumn("TRANSACTION_TYPE", col("TRANSACTION_TYPE").cast(StringType())
                           ).withColumn("TRANSACTION_VALUE", col("TRANSACTION_VALUE").cast(DoubleType())
                           ).withColumn("TRANSACTION_ID", col("TRANSACTION_ID").cast(IntegerType()))

df_cdw_sapp_credit= cdw_sapp_credit.select("CUST_CC_NO", "TIMEID","CUST_SSN","BRANCH_CODE","TRANSACTION_TYPE","TRANSACTION_VALUE","TRANSACTION_ID")

#Create a Database in SQL(MySQL), named “creditcard_capstone.”

#) Create a Python and Pyspark Program to load/write the “Credit card System Data” into RDBMS(creditcard_capstone).
# Tables should be created by the following names in RDBMS:
# CDW_SAPP_BRANCH
# CDW_SAPP_CREDIT_CARD
# CDW_SAPP_CUSTOMER

spark = SparkSession.builder.master("local[*]").appName("creditcard_capstone").getOrCreate()

# Write the spark DataFrame into creditcard_capstone database 
# Use Secret.py to create a connection with database url and credentials
#Add 3 tables 
cdw_sapp_branch.write.format("jdbc") \
  .mode("append") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
  .option("dbtable", "df_cdw_sapp_branch") \
  .option("dbtable", "df_cdw_sapp_credit") \
  .option("dbtable", "df_cdw_sapp_custmer") \
  .option("user", Secret.My_username ) \
  .option("password", Secret.My_password) \
  .save()

# # 1) Used to display the transactions made by customers living in a
# # given zip code for a given month and year. Order by day in
# # descending order.

# #def transaction()
# # Get zip_code, Month, year from the user
# zip_code = input("Please enter the Zip code: ")
# Month = input("Please enter the month: ")
# Year = input("Please enter the year")

# #Split the TIMEID in to Day, Month and Year before satrt the quary 
# df_cdw_sapp_credit.show(5)

# #use lpad function to for month and day because some of them put as one degit

# df_TIMEID = df_cdw_sapp_credit.withColumn("YEAR", substring(col("TIMEID"), 1,4).cast("VARCHAR(20)")
#                                           ).withColumn("MONTH", (substring(col("TIMEID"), 5,2).cast("VARCHAR(20)"))
#                                           ).withColumn("DAY", (substring(col("TIMEID"), 7,5).cast("VARCHAR(20)")))



# df_TIMEID.show(5)