
%run "../includes/configuration"

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
                        .withColumnRenamed("number", "driver_number") \
                        .withColumnRenamed("name", "driver_name") \
                        .withColumnRenamed("nationality", "driver_nationality")

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors") \
                            .withColumnRenamed("name", "team")

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
                        .withColumnRenamed("location", "circuit_location")

races_df = spark.read.parquet(f"{processed_folder_path}/races") \
                    .withColumnRenamed("name", "race_name") \
                    .withColumnRenamed("race_timestamp", "race_date")

results_df = spark.read.parquet(f"{processed_folder_path}/results") \
                        .withColumnRenamed("time", "race_time")

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id==circuits_df.circuit_id, "inner") \
                            .select(races_df.race_id,races_df.race_year,races_df.race_name,races_df.race_date,circuits_df.circuit_location)
