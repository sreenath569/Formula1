dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

%run "../includes/configuration"
  
%run "../includes/common_functions"

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import current_timestamp, col

results_schema = StructType([
  StructField("resultId" IntegerType(), False),
  StructField("raceId" IntegerType(), True),
  StructField("driverId" IntegerType(), True),
  StructField("constructorId" IntegerType(), True),
  StructField("number" IntegerType(), True),
  StructField("grid" IntegerType(), True),
  StructField("position" IntegerType(), True),
  StructField("positionText" StringType(), True),
  StructField("positionOrder" IntegerType(), True),
  StructField("points" FloatType(), True),
  StructField("laps" IntegerType(), True),
  StructField("time" StringType(), True),
  StructField("milliseconds" IntegerType(), True),
  StructField("fastestLap" IntegerType(), True),
  StructField("rank" IntegerType(), True),
  StructField("fastestLapTime" StringType(), True),
  StructField("fastestLapSpeed" FloatType(), True),
  StructField("statusId" StringType(), True)
])

results_df = spark.read.schema(results_schema).json(f"{raw_folder_path}/results.json")

results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_text") \
                                    .withColumnRenamed("positionOrder", "position_order") \
                                    .withColumnRenamed("fastestLap", "fastest_lap") \
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                    .withColumn("data_source", lit(v_data_source))


results_dropped_df = results_with_columns_df.drop(col("statusId"))


results_final_df = add_ingestion_date(results_dropped_df)

results_final_df.write.mode("overwrite").parquet("f"{processed_folder_path}/results")

                                                
                                    
