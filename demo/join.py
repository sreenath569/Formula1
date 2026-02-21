
%run "../includes/configuration"

races_df = spark.read.parquet(f"{processed_folder_path}/races") \
                      .withColumnRenamed("name", "race_name")

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
                        .withColumnRenamed("name", "circuit_name")

# inner join
race_circuits_df = circuits_df.join(races_df, races_df.circuit_id==circuits_df.circuit_id, "inner") \
                              .select(circuit_df.circuit_name, circuit_df.location, circuit_df.country, races_df.race_name, races_df.round)

# left outer join
race_circuits_df = circuits_df.join(races_df, races_df.circuit_id==circuits_df.circuit_id, "left") \
                              .select(circuit_df.circuit_name, circuit_df.location, circuit_df.country, races_df.race_name, races_df.round)

# right outer join
race_circuits_df = circuits_df.join(races_df, races_df.circuit_id==circuits_df.circuit_id, "right") \
                              .select(circuit_df.circuit_name, circuit_df.location, circuit_df.country, races_df.race_name, races_df.round)

# full outer join
race_circuits_df = circuits_df.join(races_df, races_df.circuit_id==circuits_df.circuit_id, "full") \
                              .select(circuit_df.circuit_name, circuit_df.location, circuit_df.country, races_df.race_name, races_df.round)

"""
A left semi join returns rows from the left dataframe/table where a match exists in the right dataframe/table.
"""

# semi join / left semi join
race_circuits_df = circuits_df.join(races_df, races_df.circuit_id==circuits_df.circuit_id, "semi")

"""
A left anti join returns rows from the left dataframe/table that do not have matching keys in the right dataframe/table.
"""

# anti join / left anti join
race_circuits_df = circuits_df.join(races_df, races_df.circuit_id==circuits_df.circuit_id, "anti")


# cross join
race_circuits_df = circuits_df.crossJoin(races_df)


