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
                        .csv("folderpath")

lap_times_final_df = lap_times_df.withColumnRenamed("raceId", "race_id") \
                               .withColumnRenamed("driverId", "driver_id") \
                               .withColumn("ingestion_date", current_timestamp())

lap_times_final_df.write.mode("overwrite").parquet("folderpath")

                                                
                                    
