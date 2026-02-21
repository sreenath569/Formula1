
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

%run "../includes/configuration"
  
%run "../includes/common_functions"

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, lit

# defining schema
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# create dataframe of data file csv
constructors_df = spark.read.schema(constructors_schema) \
                        .json(f"{raw_folder_path}/constructors.json")

constructors_dropped_df = constructors_df.drop(col("url"))

constructors_renamed_df = constructors_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                  .withColumnRenamed("constructorRef", "constructor_ref") \
                                  .withColumn("data_source", lit(v_data_source))

constructors_final_df = add_ingestion_date(constructors_renamed_df)

constructors_final_df.write.mode("overwrite").parquet("f"{processed_folder_path}/constructors")

dbutils.notebook.exit("Success")
                                                
                                    
