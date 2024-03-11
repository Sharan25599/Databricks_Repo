# Databricks notebook source
# MAGIC %run /Notebook/source_to_bronze/utils

# COMMAND ----------

#Read the employee dataframe

file_path ="dbfs:/FileStore/Employee_Q1.csv"
employee_df=read_file("csv",file_path,header=True)
display(employee_df)

# COMMAND ----------


#Read the department dataframe

file_path ="dbfs:/FileStore/Department_Q1.csv"
department_df=read_file("csv",file_path,header=True)
display(department_df)

# COMMAND ----------


#Reading the country dataframe

file_path ="dbfs:/FileStore/Country_Q1.csv"
country_df=read_file("csv", file_path, header=True)
display(country_df)

# COMMAND ----------

#Writing employee_df to source_to_bronze

file_path="dbfs:/source_to_bronze/employee_df.csv"
write_to_csv(employee_df,file_path,header=True)

# COMMAND ----------

#Writing department_df to source_to_bronze

file_path="dbfs:/source_to_bronze/department_df.csv"
write_to_csv(department_df,file_path,header=True)

# COMMAND ----------

#Writing country_df to source_to_bronze

file_path="dbfs:/source_to_bronze/country_df.csv"
write_to_csv(country_df,file_path,header=True)