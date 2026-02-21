
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

%run "../includes/configuration"
  
%run "../includes/common_functions"

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, lit

# defining schema
circuits_schema = StructType([
  StructField("circuitId" IntegerType(), False),
  StructField("circuitRef" StringType(), True),
  StructField("name" StringType(), True),
  StructField("location" StringType(), True),
  StructField("country" StringType(), True),
  StructField("lat" DoubleType(), True),
  StructField("lng" DoubleType(), True),
  StructField("alt" IntegerType(), True),
  StructField("url" StringType(), True)
])

# create dataframe of data file csv
circuits_df = spark.read.schema(circuits_schema) \
                        .csv(f"{raw_folder_path}/circuits.csv")

circuits_selected_df = circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

circuits_renamed_df = circuits_df.withColumnRenamed("circuitId", "circuit_id") \
                                  .withColumnRenamed("circuitRef", "circuit_ref") \
                                  .withColumnRenamed("lat", "latitude") \
                                  .withColumnRenamed("lng", "longitude") \
                                  .withColumnRenamed("alt", "altitude") \
                                  .withColumn("data_source", lit(v_data_source))

circuits_final_df = add_ingestion_date(circuits_renamed_df)

circuits_final_df.write.mode("overwrite").parquet("f"{processed_folder_path}/circuits")

dbutils.notebook.exit("Success")

                                                
                                    
