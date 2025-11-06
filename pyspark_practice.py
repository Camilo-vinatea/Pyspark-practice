import findspark
findspark.init()

# Import sum pyspark function to avoid crossing reference with Python's standard sum function
from pyspark.sql.functions import sum, avg
from pyspark.sql.functions import when, lit

# PySpark is the Spark API for Python. In this lab, we use PySpark to initialize the SparkContext.   
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# Import date libraries
from pyspark.sql.functions import year, quarter, to_date

# Download dataset with wget
import wget

# Creating a SparkContext object
sc = SparkContext.getOrCreate()

# Creating a Spark Session
spark = SparkSession \
    .builder \
    .appName("Python Spark DataFrames basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

link1 = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/dataset1.csv'
wget.download(link1)

link2 = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/dataset2.csv'
wget.download(link2)

# Load the data into pyspark dataframes
df1 = spark.read.csv('dataset1.csv', header=True, inferSchema=True)
df2 = spark.read.csv('dataset2.csv', header=True, inferSchema=True)

# Display the squema for each dataset
df1.printSchema()
df2.printSchema()

# Add new columns to each dataframe
    # Add new column 'year' to df1
df1 = df1.withColumn('year',year(to_date('date_column','dd/MM/yyy')))

    # Add new column 'quarter' to df2
df2 = df2.withColumn('quarter',quarter(to_date('transaction_date','dd/MM/yyy')))

# Rename columns
df1 = df1.withColumnRenamed('amount', 'transaction_amount')
df2 = df2.withColumnRenamed('value', 'transaction_value')

# Drop (remove) columns
df1 = df1.drop('description','location')
df2 = df2.drop('notes')

# Join dataframes based on a common column
joined_df = df1.join(df2,'customer_id', 'inner')

# Filter data based on a condition
filtered_df = joined_df.filter(joined_df['transaction_amount'] > 1000) # Alternative query: filtered_df = joined_df.filter("transaction_amount > 1000")

# Aggregate data by customer and show the dataframe
total_amount_per_customer = filtered_df.groupBy('customer_id').agg(sum('transaction_amount').alias('total_amount'))
total_amount_per_customer.show()

# Write the result to a Hive table: Write total_amount_per_customer to a Hive table named customer_totals.
total_amount_per_customer.write.mode("overwrite").saveAsTable("customer_totals")

# Write the filtered_df to HDFS in parquet format to a file name filtered_data
filtered_df.write.mode("overwrite").parquet("filtered_data.parquet")

# Add a new column based on a condition
'''Add a new column named high_value to df1 indicating whether the transaction_amount
is greater than 5000. When the value is greater than 5000, the value of the column
should be Yes. When the value is less than or equal to 5000, the value of the column should be No.'''
df1 = df1.withColumn('high_value', when(df1.transaction_amount > 5000, lit("Yes")).otherwise(lit("No")))

# Calculate the average transaction value per quarter
'''Calculate and display the average transaction value for each quarter in df2 and
create a new dataframe named average_value_per_quarter with column avg_trans_val.'''
    #calculate the average transaction value for each quarter in df2
average_value_per_quarter = df2.groupby('quarter').agg(avg('transaction_value').alias('avg_trans_val')).orderBy('quarter')
                                                      
    #show the average transaction value for each quarter in df2    
average_value_per_quarter.show()

# Calculate the total transaction value per year
'''Calculate and display the total transaction value for each year in df1 and create
a new dataframe named total_value_per_year with column total_transaction_val.'''
total_value_per_year = df1.groupBy('year').agg(sum('transaction_amount').alias('total_transaction_val')).orderBy('year')
total_value_per_year.show()

# Write the result to HDFS
'''Write total_value_per_year to HDFS in the CSV format to file named total_value_per_year'''
total_value_per_year.write.mode("overwrite").csv("total_value_per_year.csv")

# Terminate pyspark session
spark.stop()
