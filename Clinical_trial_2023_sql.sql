-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.fs.ls("/FileStore/tables")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fileroot = "clinicaltrial_2023"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC import os
-- MAGIC os.environ['fileroot'] = fileroot

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # introducing Reusability
-- MAGIC clincaltrial_2023 = ("/FileStore/tables/"+fileroot+".csv")
-- MAGIC pharma = ('/FileStore/tables/pharma.csv')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession, Row
-- MAGIC from pyspark.sql.types import *
-- MAGIC
-- MAGIC # Define your own schema
-- MAGIC schema = StructType([
-- MAGIC     StructField("id", StringType(), True),
-- MAGIC     StructField("Study Title", StringType(), True),
-- MAGIC     StructField("Acronym", StringType(), True),
-- MAGIC     StructField("Status", StringType(), True),
-- MAGIC     StructField("Conditions", StringType(), True),
-- MAGIC     StructField("Interventions", StringType(), True),
-- MAGIC     StructField("Sponsor", StringType(), True),
-- MAGIC     StructField("Collaborators", StringType(), True),
-- MAGIC     StructField("Enrollment", StringType(), True),
-- MAGIC     StructField("Funder Type", StringType(), True),
-- MAGIC     StructField("Type", StringType(), True),
-- MAGIC     StructField("Study Design", StringType(), True),
-- MAGIC     StructField("Start", StringType(), True),
-- MAGIC     StructField("Completion", StringType(), True)
-- MAGIC ])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinicaltrial_2023RDD = sc.textFile(clincaltrial_2023)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Remove the header row
-- MAGIC header = clinicaltrial_2023RDD.first()
-- MAGIC data_without_header = clinicaltrial_2023RDD.filter(lambda row: row != header)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Convert RDD elements to Rows and handle schema mismatch
-- MAGIC def parse_row(row):
-- MAGIC     fields = row.split("\t")
-- MAGIC     # Pad the fields to match the length of the schema
-- MAGIC     padded_fields = fields + [None] * (len(schema.fields) - len(fields))
-- MAGIC     return Row(*padded_fields)
-- MAGIC
-- MAGIC # Apply the parsing function and create RDD of Row objects
-- MAGIC rdd_rows = data_without_header.map(parse_row)
-- MAGIC
-- MAGIC # Convert RDD to DataFrame using the custom schema
-- MAGIC Clinical_trial2023DF= spark.createDataFrame(rdd_rows,schema)
-- MAGIC
-- MAGIC # Show the DataFrame
-- MAGIC Clinical_trial2023DF.show()

-- COMMAND ----------

-- MAGIC  %python  
-- MAGIC #cleaning the completion column by removing comma and double quotes
-- MAGIC from pyspark.sql.functions import regexp_replace
-- MAGIC Clinical_trial2023DF2= Clinical_trial2023DF.withColumn("Completion", regexp_replace("Completion", '"', ''))
-- MAGIC Clinical_trial2023DF3= Clinical_trial2023DF2.withColumn("Completion", regexp_replace("Completion", ',', ''))
-- MAGIC Clinical_trial2023DF3= Clinical_trial2023DF3.withColumn("id", regexp_replace("id", '"', ''))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC Clinical_trial2023DF3.display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC Clinical_trial2023DF3.createOrReplaceTempView("SqlClinical_trial2023DF")

-- COMMAND ----------

SELECT * FROM SqlClinical_trial2023DF LIMIT 20

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pharma_df = spark.read.csv(pharma,header=True,inferSchema=True,sep=",")
-- MAGIC pharma_df.show(2, truncate=False)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Register the DataFrame as a temporary view
-- MAGIC pharma_df.createOrReplaceTempView("pharma_sql")

-- COMMAND ----------

SELECT *
FROM pharma_sql
LIMIT 2;

-- COMMAND ----------

show databases

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #1) Number of Studies in the dataset

-- COMMAND ----------


SELECT  COUNT('Id') FROM SqlClinical_trial2023DF

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #2)  Type of Studies in the dataset with the frequency of Each type

-- COMMAND ----------

SELECT Type, COUNT(Type) AS frequency FROM  SqlClinical_trial2023DF
GROUP BY Type
ORDER BY frequency DESC
LIMIT 4;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #3)The top 5 conditions (from Conditions) with their frequencies.

-- COMMAND ----------

-- Splitting Conditions by delimiter and flattening the resulting array
WITH split_conditions AS (
    SELECT EXPLODE(SPLIT(Conditions, '\\|')) AS condition
    FROM SqlClinical_trial2023DF
)
-- Counting occurrences of each condition
SELECT condition, COUNT(*) AS frequency
FROM split_conditions
GROUP BY condition
ORDER BY frequency DESC
LIMIT 5;


-- COMMAND ----------


SELECT *
FROM pharma_sql
LIMIT 2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##4) Find the 10 most common sponsors that are not pharmaceutical companies, alongwith the number of clinical trials they have sponsored.

-- COMMAND ----------


SELECT Sponsor, COUNT('Sponsor') AS freq FROM SqlClinical_trial2023DF
LEFT JOIN pharma_sql ON SqlClinical_trial2023DF.Sponsor=pharma_sql.Parent_Company
WHERE pharma_sql.Parent_Company IS NULL
GROUP BY Sponsor
ORDER BY freq DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #5)Plot number of completed studies for each month in 2023

-- COMMAND ----------

SELECT YEAR(SUBSTRING(Completion, 1, 7)) AS year,
       MONTH(SUBSTRING(Completion, 1, 7)) AS month,
       COUNT(*) AS completed_studies
FROM SqlClinical_trial2023DF
WHERE Completion LIKE '2023-%' AND Status = 'COMPLETED'
GROUP BY YEAR(SUBSTRING(Completion, 1, 7)), MONTH(SUBSTRING(Completion, 1, 7))
ORDER BY YEAR(SUBSTRING(Completion, 1, 7)), MONTH(SUBSTRING(Completion, 1, 7));


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Top Collaborators with their frequency

-- COMMAND ----------

SELECT Collaborators, COUNT(*) AS count
FROM  SqlClinical_trial2023DF
WHERE Collaborators IS NOT NULL
GROUP BY Collaborators
ORDER BY count DESC
LIMIT 5;




