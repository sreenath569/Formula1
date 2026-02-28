%run "../includes/configuration"

from pyspark.sql.functions import count, countDistinct, sum

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# count
race_results_df.select(count("*"))show()

# countDistinct
race_results_df.select(countDistinct("race_name")).show()

# sum
race_results_df.select(sum("points")).show()

race_results_df.select(sum("points"), countDistinct("race_name")) \
                .withColumnRenamed("sum(points)", "total_points") \
                .withColumnRenamed("count(Distinct race_name)", "number_of_races") \
                .show()

                       


