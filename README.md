# Ayele_Data_Engineering_Project
This project is a Data Extraction, Transformation, load (ETL) proccess implemented using  with Python and PySpark. 
It proccess two datasets: Loan Application and a Credit Card dataset. In this project I use various technologies and 
libraries Python (Pandas, advanced modules, e.g., Matplotlib), SQL,Apache Spark (Spark SQL), and Python 
Visualization and Analytics libraries.   

#Here is the give Workflow Diagram of the Requirements

<img width="485" alt="Workflow Diagram of the Requirements" src="https://github.com/Ribkaay/Ayele-Data-Enginerring-Project-/assets/140378209/3137d77c-e732-4bbc-bdb6-c818884c2cf2">

# Steps 1: Data extraction
- Extract data this three data using spark .read json
- CDW_SAPP_CUSTOMER.JSON: This file has the existing customer details.
- CDW_SAPP_CREDITCARD.JSON: This file contains all credit card transaction information
- CDW_SAPP_BRANCH.JSON: Each branch’s information and details are recorded in this file
  
# Steps2: Transformation(Mapping)
Transform data based on the mapping document.Mapping Document.xlsx - Google Sheets
Clean all datas and makeig ready using python and pyspark

# STEP 3: Data loading into Database
- After all three data are clean and mapped load it into s to load data into RDBMS(SQL)
-  Then Create a Database in SQL(MySQL), named “creditcard_capstone.”
- Create a Python and Pyspark Program to load/write the “Credit Card System Data” into RDBMS(creditcard_capstone)
 
# STEP 4:Application Front-End 
- Once data is loaded into the database, from  front-end (console) to see/display data. For that, create a console-based Python program  to satisfy System Requirements.
 - Preview Transaction Details Module and Customer Details

# STEP 5: Data Analysis and Visualization
- Use python libraries such as pandas (Matplotlib)
- Sample image for Highest Number of Customers  transaction count

![State_With_Highest_Number_of_Customers](https://github.com/Ribkaay/Ayele-Data-Enginerring-Project-/assets/140378209/bf345ee3-fc89-4ca1-bf16-1dd23efe1564)


# STEP 6: LOAN Application Data API
- used  LOAN Application dataset request access from a REST API by sending an HTTP request and processing the response.
- After getting the response, load it to pyspark and load it on the database.
  
# STEP 6: Data Analysis and Visualization for LOAN Application 
- Finally create a visualization for loan application. 

 
