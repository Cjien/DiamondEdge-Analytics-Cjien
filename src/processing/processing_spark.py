# Author: Cheng-Chien Chang (B13902062)
# Project: DiamondEdge Analytics - PySpark Processing
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when, round
from pybaseball import playerid_reverse_lookup
import pandas as pd

def create_spark_session():
    return SparkSession.builder \
        .appName("DiamondEdge_Sabermetrics_Processor") \
        .master("local[*]") \
        .getOrCreate()

def process_statcast_data(spark: SparkSession, input_path: str, output_dir: str):
    print("[Processing] Loading raw data...")
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    
    MIN_PITCHES = 50
    
    # ==========================================
    # Pipeline A: Batter Metrics
    # ==========================================
    print("\n[Processing] Calculating advanced batter metrics...")
    batter_events = df.filter(col("launch_speed").isNotNull() & col("events").isNotNull())
    
    batter_stats = batter_events.groupBy("batter").agg(
        count("*").alias("total_batted_balls"),
        round(avg("launch_speed"), 2).alias("avg_exit_velocity"),
        count(when(col("launch_speed") >= 95, True)).alias("hard_hit_count")
    )
    
    value_batters = batter_stats.withColumn(
        "hard_hit_rate", 
        round(col("hard_hit_count") / col("total_batted_balls"), 3)
    ).filter(col("total_batted_balls") >= 15)
    
    # --- Reverse Lookup for Player Names ---
    print("[Processing] Translating batter IDs to player names...")
    unique_batter_ids = [row['batter'] for row in value_batters.select('batter').distinct().collect()]
    
    if unique_batter_ids:
        lookup_df = playerid_reverse_lookup(unique_batter_ids, key_type='mlbam')
        lookup_df['player_name'] = lookup_df['name_first'].str.title() + " " + lookup_df['name_last'].str.title()
        lookup_df = lookup_df[['key_mlbam', 'player_name']]
        
        lookup_spark_df = spark.createDataFrame(lookup_df)
        value_batters = value_batters.join(
            lookup_spark_df,
            value_batters.batter == lookup_spark_df.key_mlbam,
            "left"
        ).drop("key_mlbam", "batter")
    else:
        value_batters = value_batters.withColumn("player_name", col("batter"))
    
    # Filter for above-average exit velocity (> 89.0 mph)
    top_batters = value_batters.filter(col("avg_exit_velocity") > 89.0) \
                               .orderBy(col("hard_hit_rate").desc()) \
                               .select("player_name", "total_batted_balls", "avg_exit_velocity", "hard_hit_count", "hard_hit_rate")
    
    # ==========================================
    # Pipeline B: Pitcher Metrics
    # ==========================================
    print("\n[Processing] Calculating advanced pitcher metrics...")
    pitcher_stats = df.groupBy("player_name").agg(
        count("*").alias("total_pitches_thrown"),
        round(avg("release_speed"), 2).alias("avg_fastball_velocity"),
        count(when(col("description") == "swinging_strike", True)).alias("swinging_strikes")
    )
    
    value_pitchers = pitcher_stats.withColumn(
        "swstr_rate", 
        round(col("swinging_strikes") / col("total_pitches_thrown"), 3)
    ).filter(col("total_pitches_thrown") >= MIN_PITCHES)
    
    # Filter for elite whiff rates (>= 15%)
    top_pitchers = value_pitchers.filter(col("swstr_rate") >= 0.15) \
                                 .orderBy(col("swstr_rate").desc())
    
    # ==========================================
    # Save Outputs
    # ==========================================
    batters_output = f"{output_dir}/value_batters.parquet"
    pitchers_output = f"{output_dir}/value_pitchers.parquet"
    
    top_batters.write.mode("overwrite").parquet(batters_output)
    top_pitchers.write.mode("overwrite").parquet(pitchers_output)
    
    print(f"\n[Processing] Pipeline complete!")

if __name__ == "__main__":
    spark_session = create_spark_session()
    input_file = "data/raw/statcast_*.csv" 
    output_directory = "data/processed"
    process_statcast_data(spark_session, input_file, output_directory)
    spark_session.stop()