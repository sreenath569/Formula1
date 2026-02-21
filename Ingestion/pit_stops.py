
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

%run "../includes/configuration"
  
%run "../includes/common_functions"

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp

# defining schema
pit_stops_schema = StructType([
  StructField("raceId" IntegerType(), True),
  StructField("driverId" IntegerType(), True),
  StructField("stop" StringType(), True),
  StructField("lap" IntegerType(), True),
  StructField("time" StringType(), True),
  StructField("duration" StringType(), True),
  StructField("milliseconds" IntegerType(), True)
])

# create dataframe of data file multiline Json
pit_stops_df = spark.read.schema(pit_stops_schema) \
                        .option("multiLine", True) \
                        .json(f"{raw_folder_path}/pit_stops.json")

pit_stops_renamed_df = pit_stops_df.withColumnRenamed("raceId", "race_id") \
                               .withColumnRenamed("driverId", "driver_id") \
                               .withColumn("data_source", lit(v_data_source))

pit_stops_final_df = add_ingestion_date(pit_stops_renamed_df)

pit_stops_final_df.write.mode("overwrite").parquet("f"{processed_folder_path}/pit_stops")

dbutils.notebook.exit("Success")

                                                
                                    
