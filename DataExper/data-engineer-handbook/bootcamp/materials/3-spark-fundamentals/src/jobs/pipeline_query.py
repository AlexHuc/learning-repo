#!/usr/bin/env python3
"""
Job: pipeline_query.py
Description: Convert the pipeline query from PostgreSQL into a PySpark job.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, when, array, struct, concat, lit
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, BooleanType, DoubleType

def create_spark_session():
    return SparkSession.builder \
            .appName("PipelineQuery") \
            .master("local[*]") \
            .getOrCreate()

def load_tables(spark):
    # Assume CSV input for demonstration; adjust file paths as necessary.
    players = spark.read.csv("data/players.csv", header=True, inferSchema=True)
    player_seasons = spark.read.csv("data/player_seasons.csv", header=True, inferSchema=True)
    
    players.createOrReplaceTempView("players")
    player_seasons.createOrReplaceTempView("player_seasons")
    
    return players, player_seasons

def run_pipeline_query(spark):
    # Last season: players for season 1997
    last_season = spark.sql("""
        SELECT * FROM players
        WHERE current_season = 1997
    """)
    last_season.createOrReplaceTempView("last_season")
    
    # This season: player_seasons for season 1998
    this_season = spark.sql("""
        SELECT * FROM player_seasons
        WHERE season = 1998
    """)
    this_season.createOrReplaceTempView("this_season")
    
    # For the array concatenation of seasons, we assume that the players table’s seasons column is stored
    # as an array of structs. For a new record from this season we build the season stats as a struct.
    #
    # We perform a full outer join on player_name between last_season and this_season.
    full_df = last_season.alias("ls").join(
        this_season.alias("ts"),
        on="player_name",
        how="fullouter"
    )
    
    # Build the new seasons field:
    # If there is already a seasons array, we concatenate a new season record (if ts.season is not null)
    # Otherwise, we just take the new season record (or an empty array).
    # Note: In Spark we use `when` to emulate CASE statements.
    new_season_record = array(struct(
        col("ts.season").alias("season"),
        col("ts.pts").alias("pts"),
        col("ts.ast").alias("ast"),
        col("ts.reb").alias("reb"),
        col("ts.weight").alias("weight")
    ))
    
    # Assume that the seasons column in players (ls) is of type array. If not, you might need to cast or
    # initialize it as an empty array.
    new_seasons = when(
        col("ts.season").isNotNull(),
        concat(coalesce(col("ls.seasons"), array().cast("array<struct<season:int, pts:double, ast:double, reb:double, weight:double>>")), new_season_record)
    ).otherwise(coalesce(col("ls.seasons"), array().cast("array<struct<season:int, pts:double, ast:double, reb:double, weight:double>>")))
    
    result_df = full_df.select(
        coalesce(col("ls.player_name"), col("ts.player_name")).alias("player_name"),
        coalesce(col("ls.height"), col("ts.height")).alias("height"),
        coalesce(col("ls.college"), col("ts.college")).alias("college"),
        coalesce(col("ls.country"), col("ts.country")).alias("country"),
        coalesce(col("ls.draft_year"), col("ts.draft_year")).alias("draft_year"),
        coalesce(col("ls.draft_round"), col("ts.draft_round")).alias("draft_round"),
        coalesce(col("ls.draft_number"), col("ts.draft_number")).alias("draft_number"),
        new_seasons.alias("seasons"),
        when(
            col("ts.season").isNotNull(),
            when(col("ts.pts") > 20, lit("star"))
            .when(col("ts.pts") > 15, lit("good"))
            .when(col("ts.pts") > 10, lit("average"))
            .otherwise(lit("bad"))
        ).otherwise(col("ls.scoring_class")).alias("scoring_class"),
        (col("ts.season").isNotNull()).alias("is_active"),
        lit(1998).alias("current_season")
    )
    
    return result_df

def main():
    spark = create_spark_session()
    try:
        load_tables(spark)
        result_df = run_pipeline_query(spark)
        result_df.show(truncate=False)
        result_df.write.mode("overwrite").parquet("data/output_pipeline_query")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()