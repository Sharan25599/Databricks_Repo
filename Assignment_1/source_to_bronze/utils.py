# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, StringType, IntegerType
from pyspark.sql.functions import udf,lit,col,avg
from pyspark.sql.functions import current_date

# COMMAND ----------

#Function to Read the 3 datasets as dataframe

def read_file(fileformat,path,**options):
    df=spark.read.format(fileformat).options(**options).load(path)
    return df

# COMMAND ----------

#Function to Read the file located in DBFS location source_to_bronze with as data frame different read methods using custom schema

def read_csv(spark, file_path, custom_schema=None, **options):
    return spark.read.csv(file_path, schema=custom_schema, **options)

# COMMAND ----------

#function to write to a location in DBFS,as /source_to_bronze/file_name.csv (employee, department_df, country_df) as CSV format. 

def write_to_csv(df, file_path, header=True, mode="overwrite"):
    df.write.csv(file_path, header=header, mode=mode)

# COMMAND ----------

#convert the camel case of the columns to the snake case

def convert_to_snake_case(column_name):
    result = [column_name[0].lower()]
    for char in column_name[1:]:
        if char.isupper():
            result.extend(['_', char.lower()])
        else:
            result.append(char)
    return ''.join(result)

# COMMAND ----------

def convert_columns_to_snake_case_udf(df):
    for col_name in df.columns:
        new_col_name = convert_to_snake_case(col_name)  
        df = df.withColumnRenamed(col_name, new_col_name)
    return df

# COMMAND ----------

#Function to Add the load_date column with the current date

def add_load_date_column(df, colum_name):
    df = df.withColumn(colum_name,lit(current_date()))
    return df

# COMMAND ----------

#Function to write the DF as a delta table to the location /silver/db_name/table_name. 

def write_delta(df,file_format,output_path,table_name):
    df.write.format(file_format).mode("overwrite").option("path", output_path).saveAsTable(table_name)

# COMMAND ----------

#Function to Reading the table stored in a silver layer as DataFrame 

def read_delta(spark,file_format,delta_location):
    df = spark.read.format(file_format).load(delta_location)
    return df

# COMMAND ----------

#Function to Find the salary of each department in descending order

def order_salary_by_department(df, salary_column, department_column):
    return df.orderBy(col(department_column), col(salary_column).desc())

# COMMAND ----------

#Function to Find the number of employees in each department located in each country.

 def group_by_department_country(df,department_column,country_column):
    return df.groupBy(col(department_column), col(country_column)).count()

# COMMAND ----------

#List of the department names along with their corresponding country names. 

def list_department_names(department_df, country_df):
     joined_df = department_df.alias("d") \
    .join(employee_df.alias("e"), col("d.department_id") == col("e.department"), "left") \
    .join(country_df.alias("c"), col("c.country_code") == col("e.country"), "left") \
    .select("d.department_name", "c.country_name")
     return joined_df

# COMMAND ----------

#The average age of employees in each department

def avg_age_of_employee(df,department_column,age_column):
    return df.groupBy(department_column).agg(avg(age_column).alias("avg_age"))

# COMMAND ----------

#droped the column load_data

def drop_columns(df, columns_to_drop):
    return df.drop(*columns_to_drop)

# COMMAND ----------

#added the column at_load_data

def add_at_load_date(df, new_column_name):
    return df.withColumn(new_column_name, lit(current_date()))
