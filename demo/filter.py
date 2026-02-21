
%run "../includes/configuration"

races_df = spark.read.parquet(f"{processed_folder_path}/races")

races_filtered_df = races_df.filter("race_year=2019 and round<=5")
OR
races_filtered_df = races_df.where((races_df["race_year"]==2019) and (races_df["round"]<=5))

"""
In PySpark, the filter() and where() methods are aliases and are completely interchangeable. 
They perform the exact same function of filtering rows in a DataFrame based on a given condition, and there is no difference in their performance.
"""
