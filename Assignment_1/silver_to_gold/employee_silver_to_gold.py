# Databricks notebook source
# MAGIC %run /Notebook/source_to_bronze/utils

# COMMAND ----------

#Reading the delta table stored

filepath = "dbfs:/silver/employee_info/dim_employee/Employees_table"
employee_df = spark.read.format("delta").load(filepath)

# COMMAND ----------

#Find the salary of each department in descending order

employee_df1=order_salary_by_department(employee_df, salary_column="salary", department_column="department")

# COMMAND ----------

#Find the number of employees in each department located in each country.

employee_df2=group_by_department_country(employee_df,department_column="department",country_column="country")

# COMMAND ----------

#List of the department names along with their corresponding country names. 

joined_df = list_department_names(department_df, country_df)

# COMMAND ----------

#The average age of employees in each department

employee_df3= avg_age_of_employee(employee_df,department_column="department",age_column="age")

# COMMAND ----------

#droped the column load_data

employee_df4 = drop_columns(employee_df, ["load_date"])

# COMMAND ----------

#added the column at_load_data

employee_df5 = add_at_load_date(employee_df, new_column_name="at_load_date")

# COMMAND ----------

#Writing the delta file

file_format='delta'
output_path="/gold/employee/fact_employee"
table_name="fact_employee"
write_delta(employee_df,file_format,output_path,table_name)
