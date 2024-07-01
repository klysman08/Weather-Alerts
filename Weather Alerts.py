# Databricks notebook source
# MAGIC %md
# MAGIC ## Leitura dos dados em ROW

# COMMAND ----------

import requests

# URL to fetch the weather alerts
url = "https://alerts.weather.gov/cap/us.php?x=0"

# Fetch the data from the API
response = requests.get(url)
xml_data = response.content


# COMMAND ----------

import xml.etree.ElementTree as ET

# Parse the XML data
root = ET.fromstring(xml_data)


# COMMAND ----------

# Define a namespace dictionary to handle namespaces in the XML
namespaces = {'atom': 'http://www.w3.org/2005/Atom', 'cap': 'urn:oasis:names:tc:emergency:cap:1.2'}

# Extract relevant data into a list of dictionaries
alerts = []
for entry in root.findall('atom:entry', namespaces):
    alert = {
        'id': entry.find('atom:id', namespaces).text,
        'updated': entry.find('atom:updated', namespaces).text,
        'published': entry.find('atom:published', namespaces).text,
        'title': entry.find('atom:title', namespaces).text,
        'summary': entry.find('atom:summary', namespaces).text,
        'event': entry.find('cap:event', namespaces).text if entry.find('cap:event', namespaces) is not None else None,
        'sent': entry.find('cap:sent', namespaces).text if entry.find('cap:sent', namespaces) is not None else None,
        'effective': entry.find('cap:effective', namespaces).text if entry.find('cap:effective', namespaces) is not None else None,
        'expires': entry.find('cap:expires', namespaces).text if entry.find('cap:expires', namespaces) is not None else None,
        'status': entry.find('cap:status', namespaces).text if entry.find('cap:status', namespaces) is not None else None,
        'msgType': entry.find('cap:msgType', namespaces).text if entry.find('cap:msgType', namespaces) is not None else None,
        'category': entry.find('cap:category', namespaces).text if entry.find('cap:category', namespaces) is not None else None,
        'urgency': entry.find('cap:urgency', namespaces).text if entry.find('cap:urgency', namespaces) is not None else None,
        'severity': entry.find('cap:severity', namespaces).text if entry.find('cap:severity', namespaces) is not None else None,
        'certainty': entry.find('cap:certainty', namespaces).text if entry.find('cap:certainty', namespaces) is not None else None,
        'areaDesc': entry.find('cap:areaDesc', namespaces).text if entry.find('cap:areaDesc', namespaces) is not None else None,
        'polygon': entry.find('cap:polygon', namespaces).text if entry.find('cap:polygon', namespaces) is not None else None,
        # Add more fields as needed
    }
    alerts.append(alert)


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Weather Alerts with FIPS6 Mapping") \
    .getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
    StructField("id", StringType(), True),
    StructField("updated", StringType(), True),
    StructField("published", StringType(), True),
    StructField("title", StringType(), True),
    StructField("summary", StringType(), True),
    StructField("event", StringType(), True),
    StructField("sent", StringType(), True),
    StructField("effective", StringType(), True),
    StructField("expires", StringType(), True),
    StructField("status", StringType(), True),
    StructField("msgType", StringType(), True),
    StructField("category", StringType(), True),
    StructField("urgency", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("certainty", StringType(), True),
    StructField("areaDesc", StringType(), True),
    StructField("polygon", StringType(), True),
    # Add more fields as needed
])

# Convert the list of dictionaries to a DataFrame
alerts_df = spark.createDataFrame(alerts, schema)

# Show the DataFrame
alerts_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataframe Pandas

# COMMAND ----------

# Convert the PySpark DataFrame to a pandas DataFrame
alerts_pandas_df = alerts_df.toPandas()
alerts_pandas_df



# COMMAND ----------

# MAGIC %md
# MAGIC ## Camada Bronze Delta Table

# COMMAND ----------

# Camada Bronze - Raw Data
bronze_path = "/mnt/data/bronze"

# Salvar o DataFrame como uma tabela Delta
alerts_df.write.format("delta").mode("append").save(bronze_path)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Camada Silver Delta Table

# COMMAND ----------

# Camada Silver - Cleaned and Transformed Data
silver_path = "/mnt/data/silver"

# Transformações, se necessário (exemplo simplificado)
silver_df = alerts_df.select("id", "updated", "published", "title", "event", "category")

# Salvar o DataFrame transformado como uma tabela Delta
silver_df.write.format("delta").mode("overwrite").save(silver_path)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Camada Gold Delta Table

# COMMAND ----------

# Camada Gold - Enriched Data
gold_path = "/mnt/data/gold"

# Enriquecimento dos dados (exemplo simplificado)
gold_df = alerts_df.withColumn("full_title", alerts_df["title"] + " - " + alerts_df["event"])

# Salvar o DataFrame enriquecido como uma tabela Delta
gold_df.write.format("delta").mode("overwrite").save(gold_path)
