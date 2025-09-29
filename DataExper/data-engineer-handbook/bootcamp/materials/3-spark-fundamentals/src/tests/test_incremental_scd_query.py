import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Import the function from your job
from src.jobs.incremental_scd_query import run_incremental_scd_query, create_spark_session

class IncrementalSCDQueryTest(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark_session()
        # Create small fake DataFrames for players_scd and players
        scd_data = [
            ("Alice", "A", True, 2020, 2021, 2021),
            ("Bob", "B", True, 2019, 2020, 2021),
            ("Charlie", "C", False, 2018, 2018, 2021)
        ]
        players_data = [
            ("Alice", "A", True, 2022),
            ("Bob", "C", True, 2022),
            ("David", "D", True, 2022)
        ]
        scd_columns = ["player_name", "scoring_class", "is_active", "start_season", "end_season", "current_season"]
        players_columns = ["player_name", "scoring_class", "is_active", "current_season"]
        
        scd_df = cls.spark.createDataFrame(scd_data, scd_columns)
        players_df = cls.spark.createDataFrame(players_data, players_columns)
        
        scd_df.createOrReplaceTempView("players_scd")
        players_df.createOrReplaceTempView("players")
    
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
    
    def test_incremental_scd(self):
        result_df = run_incremental_scd_query(self.spark)
        # Here you can define your expected set of player names
        expected_players = {"Alice", "Bob", "Charlie", "David"}
        result_names = set(row["player_name"] for row in result_df.collect())
        self.assertEqual(result_names, expected_players)

if __name__ == "__main__":
    unittest.main()