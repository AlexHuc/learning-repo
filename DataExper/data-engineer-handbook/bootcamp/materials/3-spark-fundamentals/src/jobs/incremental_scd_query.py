#!/usr/bin/env python3
"""
Job: incremental_scd_query.py
Description: Convert the incremental SCD query from PostgreSQL into a PySpark job.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, array, struct, expr

def create_spark_session():
    return SparkSession.builder \
            .appName("IncrementalSCDQuery") \
            .master("local[*]") \
            .getOrCreate()

def load_tables(spark):
    # In a real job these would be loaded from external sources.
    # For this example, assume CSV files are in "data/".
    players_scd = spark.read.csv("data/players_scd.csv", header=True, inferSchema=True)
    players    = spark.read.csv("data/players.csv", header=True, inferSchema=True)
    
    players_scd.createOrReplaceTempView("players_scd")
    players.createOrReplaceTempView("players")
    
    return players_scd, players

def run_incremental_scd_query(spark):
    # Create temporary views for filtering
    # Last season SCD: players from 2021 with end_season = 2021
    last_season_scd = spark.sql("""
        SELECT * FROM players_scd
        WHERE current_season = 2021 AND end_season = 2021
    """)
    last_season_scd.createOrReplaceTempView("last_season_scd")
    
    # Historical SCD: players from 2021 with end_season < 2021
    historical_scd = spark.sql("""
        SELECT player_name, scoring_class, is_active, start_season, end_season 
        FROM players_scd
        WHERE current_season = 2021 AND end_season < 2021
    """)
    historical_scd.createOrReplaceTempView("historical_scd")
    
    # This season data: players for current_season 2022
    this_season_data = spark.sql("""
        SELECT * FROM players
        WHERE current_season = 2022
    """)
    this_season_data.createOrReplaceTempView("this_season_data")
    
    # Unchanged records: join this season with last season where key attributes match
    unchanged_records = spark.sql("""
        SELECT ts.player_name,
               ts.scoring_class,
               ts.is_active,
               ls.start_season,
               ts.current_season AS end_season
        FROM this_season_data ts
        JOIN last_season_scd ls ON ts.player_name = ls.player_name
        WHERE ts.scoring_class = ls.scoring_class
          AND ts.is_active = ls.is_active
    """)
    unchanged_records.createOrReplaceTempView("unchanged_records")
    
    # Changed records: for players where attributes have changed.
    # We emulate the UNNEST of two records by creating two DataFrames and unioning them.
    # First, join to get last_season info.
    changed_join = spark.sql("""
        SELECT ts.player_name,
               ls.scoring_class AS old_scoring_class,
               ls.is_active AS old_is_active,
               ls.start_season AS old_start_season,
               ls.end_season AS old_end_season,
               ts.scoring_class AS new_scoring_class,
               ts.is_active AS new_is_active,
               ts.current_season AS new_season
        FROM this_season_data ts
        LEFT JOIN last_season_scd ls ON ts.player_name = ls.player_name
        WHERE ls.player_name IS NOT NULL
          AND (ts.scoring_class <> ls.scoring_class OR ts.is_active <> ls.is_active)
    """)
    
    # Build two branches: one for the last season record and one for the new season record.
    changed_old = changed_join.select(
        col("player_name"),
        col("old_scoring_class").alias("scoring_class"),
        col("old_is_active").alias("is_active"),
        col("old_start_season").alias("start_season"),
        col("old_end_season").alias("end_season")
    )
    
    changed_new = changed_join.select(
        col("player_name"),
        col("new_scoring_class").alias("scoring_class"),
        col("new_is_active").alias("is_active"),
        col("new_season").alias("start_season"),
        col("new_season").alias("end_season")
    )
    
    changed_records = changed_old.union(changed_new)
    changed_records.createOrReplaceTempView("changed_records")
    
    # New records: players that do not exist in last_season_scd
    new_records = spark.sql("""
        SELECT ts.player_name,
               ts.scoring_class,
               ts.is_active,
               ts.current_season AS start_season,
               ts.current_season AS end_season
        FROM this_season_data ts
        LEFT JOIN last_season_scd ls ON ts.player_name = ls.player_name
        WHERE ls.player_name IS NULL
    """)
    new_records.createOrReplaceTempView("new_records")
    
    # Final query: union all pieces and add current_season constant (2022)
    final_df = spark.sql("""
        SELECT player_name, scoring_class, is_active, start_season, end_season, 2022 as current_season
        FROM historical_scd
        UNION ALL
        SELECT player_name, scoring_class, is_active, start_season, end_season FROM unchanged_records
        UNION ALL
        SELECT player_name, scoring_class, is_active, start_season, end_season FROM changed_records
        UNION ALL
        SELECT player_name, scoring_class, is_active, start_season, end_season FROM new_records
    """)
    return final_df

def main():
    spark = create_spark_session()
    try:
        load_tables(spark)
        final_df = run_incremental_scd_query(spark)
        
        # For demonstration purposes: show the result and write out the final data
        final_df.show(truncate=False)
        final_df.write.mode("overwrite").parquet("data/output_incremental_scd")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()