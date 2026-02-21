
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

%run "../includes/configuration"
  
%run "../includes/common_functions"

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col, lit

# defining schema
races_schema = StructType([
  StructField("raceId" IntegerType(), False),
  StructField("year" IntegerType(), True),
  StructField("round" IntegerType(), True),
  StructField("circuitId" IntegerType(), True),
  StructField("name" StringType(), True),
  StructField("date" DateType(), True),
  StructField("time" StringType(), True),
  StructField("url" StringType(), True)
])

# create dataframe of data file csv
races_df = spark.read.option("header", True) \
                      .schema(races_schema) \
                      .csv(f"{raw_folder_path}/races.csv")

races_with_timestamp_df = races_df.withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time"), "yyyy-MM-dd HH:mm:ss")))

races_selected_df = races_with_timestamp_df.select(col("raceId").alias("race_id"),col("year").alias("race_year"),col("round"),col("circuitId").alias("circuit_id"),col("name"),col("race_timestamp"))

races_selected_df = races_selected_df.withColumn("data_source", lit(v_data_source))

races_final_df = add_ingestion_date(races_selected_df)

races_final_df.write.mode("overwrite").partitionBy("race_year").parquet("f"{processed_folder_path}/races")

dbutils.notebook.exit("Success")

                                                
                                    
