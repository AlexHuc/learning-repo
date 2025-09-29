import unittest
from pyspark.sql import SparkSession
from src.jobs.pipeline_query import run_pipeline_query, create_spark_session

class PipelineQueryTest(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark_session()
        # Create small fake DataFrames for players and player_seasons
        players_data = [
            ("Alice", 180, "SomeCollege", "USA", 2005, 1, 5, 
             [{"season":1997, "pts":15.0, "ast":5.0, "reb":7.0, "weight":180.0}], "good", True, 1997),
            ("Bob", 190, "OtherCollege", "USA", 2003, 2, 20, 
             [{"season":1997, "pts":22.0, "ast":4.0, "reb":10.0, "weight":200.0}], "star", True, 1997)
        ]
        players_columns = ["player_name", "height", "college", "country", "draft_year",
                           "draft_round", "draft_number", "seasons", "scoring_class", "is_active", "current_season"]
        
        player_seasons_data = [
            ("Alice", 1998, 25.0, 6.0, 8.0, 182.0),
            ("Charlie", 1998, 10.0, 2.0, 3.0, 175.0)
        ]
        player_seasons_columns = ["player_name", "season", "pts", "ast", "reb", "weight"]
        
        players_df = cls.spark.createDataFrame(players_data, schema=players_columns)
        player_seasons_df = cls.spark.createDataFrame(player_seasons_data, schema=player_seasons_columns)
        
        players_df.createOrReplaceTempView("players")
        player_seasons_df.createOrReplaceTempView("player_seasons")
    
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
    
    def test_pipeline_query(self):
        result_df = run_pipeline_query(self.spark)
        result = result_df.collect()
        # Here, you can check expected values. For example, verify that current_season is 1998
        for row in result:
            self.assertEqual(row["current_season"], 1998)

if __name__ == "__main__":
    unittest.main()
