
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

%run "../includes/configuration"
  
%run "../includes/common_functions"

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col, concat

# defining schema
name_schema = StructType([
  StructField("forname" StringType(), True),
  StructField("surname" StringType(), True)  
])

drivers_schema = StructType([
  StructField("driverId" IntegerType(), False),
  StructField("driverRef" StringType(), True),
  StructField("number" IntegerType(), True),
  StructField("code" StringType(), True),
  StructField("name" name_schema),
  StructField("dob" DateType(), True),
  StructField("nationality" StringType(), True),
  StructField("url" StringType(), True)
])

# create dataframe of data file csv
drivers_df = spark.read.schema(drivers_schema) \
                        .json(f"{raw_folder_path}/drivers.json")

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))
                                    .withColumn("data_source", lit(v_data_source))

drivers_drop_df = drivers_with_columns_df.drop(col("url"))

drivers_final_df = add_ingestion_date(drivers_drop_df)

drivers_final_df.write.mode("overwrite").parquet("f"{processed_folder_path}/drivers")

dbutils.notebook.exit("Success")
