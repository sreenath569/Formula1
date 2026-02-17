from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp

# defining schema
pitstops_schema = StructType([
  StructField("raceId" IntegerType(), True),
  StructField("driverId" IntegerType(), True),
  StructField("stop" StringType(), True),
  StructField("lap" IntegerType(), True),
  StructField("time" StringType(), True),
  StructField("duration" StringType(), True),
  StructField("milliseconds" IntegerType(), True)
])

# create dataframe of data file multiline Json
pitstops_df = spark.read.schema(pitstops_schema) \
                        .option("multiLine", True) \
                        .json("path")

pitstops_final_df = pitstops_df.withColumnRenamed("raceId", "race_id") \
                               .withColumnRenamed("driverId", "driver_id") \
                               .withColumn("ingestion_date", current_timestamp())

pitstops_final_df.write.mode("overwrite").parquet("path")

                                                
                                    
