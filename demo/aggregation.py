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

# groupBy - only one aggregate function
race_results_df.groupBy("driver_name") \
                .sum("points") \
                .show()

# groupBy & agg
race_results_df.groupBy("driver_name") \
                .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races")) \
                .show()

grouped_df = race_results_df.groupBy("race_year", "driver_name") \
                .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races")) \
                .show()

# window functions
from pyspark.sql.functions import desc
from pyspark.sql.window import Window

driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))
demo_grouped_df.withColumn("rank", rank().over(driverRankSpec)).show()

                       


