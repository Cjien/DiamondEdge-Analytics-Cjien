# Author: 張正謙 (B13902062)
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
    print("[Processing] 載入原始數據...")
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    
    MIN_PITCHES = 50
    
    # ==========================================
    # 管線 A: 打者數據 (Batters)
    # ==========================================
    print("\n[Processing] 開始計算打者進階數據 (Batters)...")
    
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
    
    # ------------------------------------------
    # ID 轉換姓名邏輯 (Reverse Lookup)
    # ------------------------------------------
    print("[Processing] 正在將打者 ID 轉換為真實姓名...")
    # 提取所有不重複的打者 ID
    unique_batter_ids = [row['batter'] for row in value_batters.select('batter').distinct().collect()]
    
    if unique_batter_ids:
        # 使用 pybaseball 查詢球員名冊
        lookup_df = playerid_reverse_lookup(unique_batter_ids, key_type='mlbam')
        # 將姓與名合併，並將首字母大寫 (title case)
        lookup_df['player_name'] = lookup_df['name_first'].str.title() + " " + lookup_df['name_last'].str.title()
        lookup_df = lookup_df[['key_mlbam', 'player_name']]
        
        # 將 Pandas DataFrame 轉回 Spark DataFrame 進行 Join
        lookup_spark_df = spark.createDataFrame(lookup_df)
        value_batters = value_batters.join(
            lookup_spark_df,
            value_batters.batter == lookup_spark_df.key_mlbam,
            "left"
        ).drop("key_mlbam", "batter") # 丟棄 ID 欄位，保留乾淨的 player_name
    else:
        # 若無資料的防呆機制
        value_batters = value_batters.withColumn("player_name", col("batter"))
    
    # 找出擊球初速高於聯盟平均 (89.0) 的打者，並重排欄位順序使其美觀
    top_batters = value_batters.filter(col("avg_exit_velocity") > 89.0) \
                               .orderBy(col("hard_hit_rate").desc()) \
                               .select("player_name", "total_batted_balls", "avg_exit_velocity", "hard_hit_count", "hard_hit_rate")
    
    print(">>> Top 10 高價值打者 (Batters by Hard Hit Rate):")
    top_batters.show(10, truncate=False)
    
    # ==========================================
    # 管線 B: 投手數據 (Pitchers)
    # ==========================================
    print("\n[Processing] 開始計算投手進階數據 (Pitchers)...")
    
    pitcher_stats = df.groupBy("player_name").agg(
        count("*").alias("total_pitches_thrown"),
        round(avg("release_speed"), 2).alias("avg_fastball_velocity"),
        count(when(col("description") == "swinging_strike", True)).alias("swinging_strikes")
    )
    
    value_pitchers = pitcher_stats.withColumn(
        "swstr_rate", 
        round(col("swinging_strikes") / col("total_pitches_thrown"), 3)
    ).filter(col("total_pitches_thrown") >= MIN_PITCHES)
    
    top_pitchers = value_pitchers.filter(col("swstr_rate") >= 0.15) \
                                 .orderBy(col("swstr_rate").desc())
    
    print(">>> Top 10 高價值投手 (Pitchers by Whiff Rate):")
    top_pitchers.show(10, truncate=False)
    
    # ==========================================
    # 儲存結果
    # ==========================================
    batters_output = f"{output_dir}/value_batters.parquet"
    pitchers_output = f"{output_dir}/value_pitchers.parquet"
    
    top_batters.write.mode("overwrite").parquet(batters_output)
    top_pitchers.write.mode("overwrite").parquet(pitchers_output)
    
    print(f"\n[Processing] 處理完成！")
    print(f"打者數據已儲存至: {batters_output}")
    print(f"投手數據已儲存至: {pitchers_output}")

if __name__ == "__main__":
    spark_session = create_spark_session()
    input_file = "data/raw/statcast_*.csv" 
    output_directory = "data/processed"
    process_statcast_data(spark_session, input_file, output_directory)
    spark_session.stop()