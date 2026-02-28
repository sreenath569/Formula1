%run "../includes/configuration"

from pyspark.sql.functions import count, countDistinct, sum

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")


