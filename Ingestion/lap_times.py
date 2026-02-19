
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

%run "../includes/configuration"
  
%run "../includes/common_functions"

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp

# defining schema
lap_times_schema = StructType([
  StructField("raceId" IntegerType(), True),
  StructField("driverId" IntegerType(), True),
  StructField("lap" IntegerType(), True),
  StructField("position" IntegerType(), True),
  StructField("time" StringType(), True),
  StructField("milliseconds" IntegerType(), True)
])

# create dataframe of data file multiline Json
lap_times_df = spark.read.schema(lap_times_schema) \
                        .csv(f"{raw_folder_path}/lap_times.csv")

lap_times_renamed_df = lap_times_df.withColumnRenamed("raceId", "race_id") \
                               .withColumnRenamed("driverId", "driver_id") \
                               .withColumn("data_source", lit(v_data_source))

lap_times_final_df = add_ingestion_date(lap_times_renamed_df)

lap_times_final_df.write.mode("overwrite").parquet("f"{processed_folder_path}/lap_times")

                                                
                                    
