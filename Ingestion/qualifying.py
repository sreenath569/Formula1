from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp

# defining schema
qualifying_schema = StructType([
  StructField("qualifyId" IntegerType(), True),
  StructField("raceId" IntegerType(), True),
  StructField("driverId" IntegerType(), True),
  StructField("constructorId" IntegerType(), True),
  StructField("number" IntegerType(), True),
  StructField("position" IntegerType(), True),
  StructField("q1" StringType(), True),
  StructField("q2" StringType(), True),
  StructField("q3" StringType(), True)
])

# create dataframe of data file multiline Json
qualifying_df = spark.read.schema(qualifying_schema) \
                        .option("multiLine", True) \
                        .json("raw/qualifying")

qualifying_final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
                                  .withColumnRenamed("raceId", "race_id") \
                                  .withColumnRenamed("driverId", "driver_id") \
                                  .withColumnRenamed("constructorId", "constructor_id") \
                                  .withColumn("ingestion_date", current_timestamp())

qualifying_final_df.write.mode("overwrite").parquet("processed/qualifying")

                                                
                                    
