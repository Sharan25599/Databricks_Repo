# Databricks notebook source
# MAGIC %run /Notebook/source_to_bronze/utils

# COMMAND ----------

#Reading the dataframes with custom schema, from dbfs location from source_to_bronze.

employee_schema=StructType([
    StructField("EmployeeID",IntegerType(),True),
    StructField("EmployeeName",StringType(),True),
    StructField("Department", StringType(), True),
    StructField("Country",StringType(), True),
    StructField("Salary",IntegerType(), True),
    StructField("Age",IntegerType(), True)
])

# COMMAND ----------

department_schema=StructType([
    StructField("DepartmentID",StringType(),True),
    StructField("DepartmentName",StringType(),True),
])

# COMMAND ----------

country_schema=StructType([
    StructField("CountryCode",StringType(),True),
    StructField("CountryName",StringType(),True)
])

# COMMAND ----------

#Reading the dataframes from source_to_bronze.

file_path = "dbfs:/source_to_bronze/eployee_df.csv"
options={'header':True}
custom_schema=employee_schema
employee_df=read_csv(spark,file_path,custom_schema,**options)

# COMMAND ----------

file_path = "dbfs:/source_to_bronze/department_df.csv"
options={'header':True}
custom_schema=department_schema
department_df=read_csv(spark,file_path,custom_schema,**options)

# COMMAND ----------

file_path = "dbfs:/source_to_bronze/contry_df.csv"
options={'header':True}
custom_schema=country_schema
country_df=read_csv(spark,file_path,custom_schema,**options)

# COMMAND ----------

#Convert the dataframes columns to snake_case

employee_df = convert_columns_to_snake_case_udf(employee_df)

# COMMAND ----------

department_df = convert_columns_to_snake_case_udf(department_df)

# COMMAND ----------

country_df = convert_columns_to_snake_case_udf(country_df)

# COMMAND ----------

#added load_data column

employee_df=add_load_date_column(employee_df, "load_date")

# COMMAND ----------

#Write dataframe to delta table

file_format='delta'
output_path="/silver/employee_info/dim_employee/Employees_table"
table_name="Employees_table"
write_delta(employee_df,file_format,output_path,table_name)