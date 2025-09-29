from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, avg, desc
import shutil
import os
import time
import logging

# ---------------------------
# Configuration and Logging
# ---------------------------
BUCKET_COUNT = 16
DATA_DIR = "../../data"
WAREHOUSE_DIR = "spark-warehouse"

# Initialize logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------------------
# Helper Functions
# ---------------------------
def safe_remove_dir(path):
    """Remove directory if it exists, with logging."""
    if os.path.exists(path):
        try:
            shutil.rmtree(path)
            logging.info(f"Removed existing directory: {path}")
        except Exception as e:
            logging.error(f"Error while removing directory {path}: {e}")

def get_file_size(path):
    """Get the sorted list of file sizes in a directory."""
    sizes = []
    if os.path.exists(path):
        for file in os.listdir(path):
            sizes.append(os.path.getsize(os.path.join(path, file)))
    return sorted(sizes)

# ---------------------------
# Spark DataFrame Functions
# ---------------------------
def broadcast_join_maps(spark):
    """Join matches with maps using explicit broadcast on maps."""
    match_df = spark.read.csv(os.path.join(DATA_DIR, "matches.csv"), header=True, inferSchema=True)
    maps_df = spark.read.csv(os.path.join(DATA_DIR, "maps.csv"), header=True, inferSchema=True)
    
    # Use explicit broadcast on maps
    matches_maps_df = match_df.join(broadcast(maps_df), "mapid")
    logging.info("Completed broadcast join between matches and maps")
    return matches_maps_df

def broadcast_join_medals(spark):
    """Join medal_matches_players with medals using explicit broadcast on medals."""
    medals_df = spark.read.csv(os.path.join(DATA_DIR, "medals.csv"), header=True, inferSchema=True)
    medals_matches_players_df = spark.read.csv(os.path.join(DATA_DIR, "medals_matches_players.csv"), header=True, inferSchema=True)
    
    # Use explicit broadcast on medals table
    medals_matches_df = medals_matches_players_df.join(broadcast(medals_df), "medal_id")
    logging.info("Completed broadcast join between medals_matches_players and medals")
    return medals_matches_df

def bucketed_join_matches(spark):
    """Bucket the matches DataFrame on match_id using BUCKET_COUNT buckets."""
    table_name = "matches_bucketed"
    table_path = os.path.join(WAREHOUSE_DIR, table_name)
    safe_remove_dir(table_path)
    
    match_df = spark.read.csv(os.path.join(DATA_DIR, "matches.csv"), header=True, inferSchema=True)
    # Repartition and sort for bucketing
    match_df = match_df.repartition(BUCKET_COUNT, "match_id").sortWithinPartitions("match_id")
    match_df.write.format("parquet").bucketBy(BUCKET_COUNT, "match_id").mode("overwrite").saveAsTable(table_name)
    logging.info(f"Bucketed matches table saved as {table_name}")
    
    return spark.table(table_name)

def bucketed_join_match_details(spark):
    """Bucket the match_details DataFrame on match_id using BUCKET_COUNT buckets."""
    table_name = "matches_bucketed_details"
    table_path = os.path.join(WAREHOUSE_DIR, table_name)
    safe_remove_dir(table_path)
    
    match_details_df = spark.read.csv(os.path.join(DATA_DIR, "match_details.csv"), header=True, inferSchema=True)
    match_details_df = match_details_df.repartition(BUCKET_COUNT, "match_id").sortWithinPartitions("match_id")
    match_details_df.write.format("parquet").bucketBy(BUCKET_COUNT, "match_id").mode("overwrite").saveAsTable(table_name)
    logging.info(f"Bucketed match_details table saved as {table_name}")
    
    return spark.table(table_name)

def bucketed_join_medals_matches_players(spark):
    """Bucket the medals_matches_players DataFrame on match_id using BUCKET_COUNT buckets."""
    table_name = "medals_matches_players_bucketed"
    table_path = os.path.join(WAREHOUSE_DIR, table_name)
    safe_remove_dir(table_path)
    
    medals_matches_players_df = spark.read.csv(os.path.join(DATA_DIR, "medals_matches_players.csv"), header=True, inferSchema=True)
    medals_matches_players_df = medals_matches_players_df.repartition(BUCKET_COUNT, "match_id").sortWithinPartitions("match_id")
    medals_matches_players_df.write.format("parquet").bucketBy(BUCKET_COUNT, "match_id").mode("overwrite").saveAsTable(table_name)
    logging.info(f"Bucketed medals_matches_players table saved as {table_name}")
    
    return spark.table(table_name)

def bucket_join_everything(matches_df, match_details_df, medals_matches_players_df):
    """Perform bucket join on the three DataFrames with match_id (and player_gamertag for medals)."""
    bucketed_df = matches_df.join(match_details_df, "match_id") \
                            .join(medals_matches_players_df, ["match_id", "player_gamertag"])
    return bucketed_df

# ---------------------------
# Aggregation & Analysis Functions
# ---------------------------
def get_aggregated_stats(spark, df):
    """Perform aggregations to answer several analytical questions."""
    start_time = time.time()
    
    # 1. Which player averages the most kills per game?
    avg_kills_df = df.groupBy("player_gamertag").agg(avg("player_total_kills").alias("average_kills_per_game"))
    top_killer = avg_kills_df.orderBy(desc("average_kills_per_game")).first()
    logging.info(f"Player with highest average kills: {top_killer['player_gamertag']} - Average: {top_killer['average_kills_per_game']}")
    
    # 2. Which playlist gets played the most?
    most_played_playlist = df.groupBy("playlist_id").count().orderBy(desc("count")).first()
    logging.info(f"Most played playlist: {most_played_playlist['playlist_id']} with count {most_played_playlist['count']}")
    
    # 3. Which map gets played the most?
    most_played_map = df.groupBy("mapid").count().orderBy(desc("count")).first()
    logging.info(f"Most played map: {most_played_map['mapid']} with count {most_played_map['count']}")
    
    # 4. Which map do players get the most Killing Spree medals on?
    # Join with medals table (broadcast join) for classification details
    medals_df = spark.read.csv(os.path.join(DATA_DIR, "medals.csv"), header=True, inferSchema=True)
    df_with_medals = df.join(broadcast(medals_df), "medal_id")
    killing_spree_df = df_with_medals.filter(col("classification") == "KillingSpree")
    most_killing_spree_map = killing_spree_df.groupBy("mapid").count().orderBy(desc("count")).first()
    logging.info(f"Map with most Killing Spree medals: {most_killing_spree_map['mapid']} with count {most_killing_spree_map['count']}")
    
    end_time = time.time()
    logging.info(f"Total time taken to aggregate stats: {end_time - start_time:.2f} seconds")

def bucketed_join_matches_v2(spark):
    """Bucket matches using match_id and sort within partitions by match_id and mapid."""
    table_name = "matches_bucketed_v2"
    table_path = os.path.join(WAREHOUSE_DIR, table_name)
    safe_remove_dir(table_path)
    
    match_df = spark.read.csv(os.path.join(DATA_DIR, "matches.csv"), header=True, inferSchema=True)
    match_df = match_df.repartition(BUCKET_COUNT, "match_id").sortWithinPartitions(["match_id", "mapid"])
    match_df.write.format("parquet").bucketBy(BUCKET_COUNT, "match_id").mode("overwrite").saveAsTable(table_name)
    logging.info(f"Bucketed matches (v2) table saved as {table_name}")
    
    return spark.table(table_name)

def bucketed_join_matches_v3(spark):
    """Bucket matches using match_id and sort within partitions by match_id and playlist_id."""
    table_name = "matches_bucketed_v3"
    table_path = os.path.join(WAREHOUSE_DIR, table_name)
    safe_remove_dir(table_path)
    
    match_df = spark.read.csv(os.path.join(DATA_DIR, "matches.csv"), header=True, inferSchema=True)
    match_df = match_df.repartition(BUCKET_COUNT, "match_id").sortWithinPartitions(["match_id", "playlist_id"])
    match_df.write.format("parquet").bucketBy(BUCKET_COUNT, "match_id").mode("overwrite").saveAsTable(table_name)
    logging.info(f"Bucketed matches (v3) table saved as {table_name}")
    
    return spark.table(table_name)

def bucketed_join_matches_v4(spark):
    """Bucket matches using match_id and sort within partitions by match_id, playlist_id, and mapid."""
    table_name = "matches_bucketed_v4"
    table_path = os.path.join(WAREHOUSE_DIR, table_name)
    safe_remove_dir(table_path)
    
    match_df = spark.read.csv(os.path.join(DATA_DIR, "matches.csv"), header=True, inferSchema=True)
    match_df = match_df.repartition(BUCKET_COUNT, "match_id").sortWithinPartitions(["match_id", "playlist_id", "mapid"])
    match_df.write.format("parquet").bucketBy(BUCKET_COUNT, "match_id").mode("overwrite").saveAsTable(table_name)
    logging.info(f"Bucketed matches (v4) table saved as {table_name}")
    
    return spark.table(table_name)

def compare_files():
    """Compare file sizes of the different bucketed tables."""
    tables = ["matches_bucketed", "matches_bucketed_v2", "matches_bucketed_v3", "matches_bucketed_v4"]
    for table in tables:
        file_path = os.path.join(WAREHOUSE_DIR, table)
        sizes = get_file_size(file_path)
        logging.info(f"Table: {table}, File Sizes (bytes): {sizes}")

def verify_table_correctness(spark):
    """Prints out basic information from the bucketed tables to verify data correctness."""
    tables = ["matches_bucketed", "matches_bucketed_details", "medals_matches_players_bucketed"]
    for table in tables:
        df = spark.table(table)
        num_partitions = df.rdd.getNumPartitions()
        count = df.count()
        logging.info(f"Table: {table} | Partitions: {num_partitions} | Number of records: {count}")
        df.explain()

# ---------------------------
# Spark Session Creation
# ---------------------------
def create_spark_session():
    spark = SparkSession.builder \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .config("spark.task.cpus", "1") \
        .config("spark.ui.port", "4050") \
        .config("spark.driver.extraJavaOptions", "-XX:-UseCompressedOops") \
        .appName("MatchStatus") \
        .master("local[2]") \
        .getOrCreate()
    logging.info("Spark session created with manual broadcast disablement")
    return spark

# ---------------------------
# Main Function
# ---------------------------
def main():
    spark = create_spark_session()
    try:
        # Step 1: Perform broadcast joins for maps and medals
        broadcast_map_df = broadcast_join_maps(spark)
        broadcast_medals_df = broadcast_join_medals(spark)
        
        # Step 2: Bucket and join the datasets
        bucketed_matches_df = bucketed_join_matches(spark)
        bucketed_match_details_df = bucketed_join_match_details(spark)
        bucketed_medals_matches_players_df = bucketed_join_medals_matches_players(spark)
        
        # Bucket join everything on match_id and player_gamertag
        bucketed_df = bucket_join_everything(bucketed_matches_df, bucketed_match_details_df, bucketed_medals_matches_players_df)
        
        # Step 3: Aggregated statistics on the joined dataset
        logging.info("=== Aggregated Stats for Bucketed DF ===")
        get_aggregated_stats(spark, bucketed_df)
        
        # Step 4: Evaluate different bucketed join strategies based on sortWithinPartitions
        # Version 2
        bucketed_matches_v2 = bucketed_join_matches_v2(spark)
        bucketed_df_v2 = bucket_join_everything(bucketed_matches_v2, bucketed_match_details_df, bucketed_medals_matches_players_df)
        logging.info("=== Aggregated Stats for Bucketed DF v2 ===")
        get_aggregated_stats(spark, bucketed_df_v2)
        
        # Version 3
        bucketed_matches_v3 = bucketed_join_matches_v3(spark)
        bucketed_df_v3 = bucket_join_everything(bucketed_matches_v3, bucketed_match_details_df, bucketed_medals_matches_players_df)
        logging.info("=== Aggregated Stats for Bucketed DF v3 ===")
        get_aggregated_stats(spark, bucketed_df_v3)
        
        # Version 4
        bucketed_matches_v4 = bucketed_join_matches_v4(spark)
        bucketed_df_v4 = bucket_join_everything(bucketed_matches_v4, bucketed_match_details_df, bucketed_medals_matches_players_df)
        logging.info("=== Aggregated Stats for Bucketed DF v4 ===")
        get_aggregated_stats(spark, bucketed_df_v4)
        
        # Step 5: Compare file sizes among the bucketed tables and verify table correctness
        compare_files()
        verify_table_correctness(spark)
    
    finally:
        # Allow time for any asynchronous logging to complete before shutting down Spark
        time.sleep(5)
        spark.stop()
        logging.info("Spark session stopped.")

if __name__ == "__main__":
    main()
