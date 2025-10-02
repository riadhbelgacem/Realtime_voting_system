import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, current_timestamp, window, 
    sum as _sum, avg, count, year, 
    when, expr, to_json, struct
)
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    IntegerType, TimestampType, DateType
)

if __name__ == "__main__":
    # Get Kafka bootstrap servers from environment
    # Use broker:9092 when running in Docker, localhost:29092 when running locally
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
    print(f"Using Kafka servers: {kafka_servers}")
    
    # Simplified Spark setup for Windows - no external jars, use Docker Spark instead
    print("\n" + "="*80)
    print("IMPORTANT: Spark Streaming should run in Docker for best compatibility!")
    print("Run this command instead:")
    print(f"docker-compose exec spark-master /opt/spark/bin/spark-submit \\")
    print(f"  --master spark://spark-master:7077 \\")
    print(f"  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\")
    print(f"  /app/spark-streaming.py")
    print("="*80 + "\n")
    
    # For local testing, use minimal config
    spark = (SparkSession.builder
        .appName("RealtimeVotingEngineering")
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate())
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Define the schema for vote data
    vote_schema = StructType([
        StructField("voter_id", StringType(), True),
        StructField("voter_name", StringType(), True),
        StructField("candidate_id", StringType(), True),
        StructField("candidate_name", StringType(), True),
        StructField("voting_time", StringType(), True),
        StructField("vote", IntegerType(), True),
        StructField("party_affiliation", StringType(), True),
        StructField("biography", StringType(), True),
        StructField("campaign_platform", StringType(), True),
        StructField("photo_url", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("nationality", StringType(), True),
        StructField("registration_number", StringType(), True),
        StructField("address", StructType([
            StructField("street", StringType(), True),   
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("country", StringType(), True),
            StructField("postcode", StringType(), True)
        ]), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("cell_number", StringType(), True),
        StructField("picture", StringType(), True),          
        StructField("registered_age", IntegerType(), True)
    ])
    
    # Read from Kafka with optimized settings for real-time processing
    votes_df = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", "votes_topic")
        .option("startingOffsets", "latest")  # Only process new data
        .option("maxOffsetsPerTrigger", "1000")  # Smaller batches for faster processing
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), vote_schema).alias("data"))
        .select("data.*")
        .filter(col("candidate_id").isNotNull())
    )
    
    # Convert voting_time string to timestamp and ensure vote is integer
    votes_df = (votes_df
        .withColumn("voting_time", col("voting_time").cast(TimestampType()))
        .withColumn("vote", col("vote").cast(IntegerType()))
        .filter(col("voting_time").isNotNull())
        .filter(col("vote").isNotNull())
    )
    
    # Calculate age from date_of_birth and create age groups
    votes_df = votes_df.withColumn(
        "birth_year",
        year(col("date_of_birth").cast(DateType()))
    ).withColumn(
        "voter_age",
        expr("2025 - birth_year")
    ).withColumn(
        "age_group",
        when(col("voter_age") < 25, "18-24")
        .when((col("voter_age") >= 25) & (col("voter_age") < 35), "25-34")
        .when((col("voter_age") >= 35) & (col("voter_age") < 45), "35-44")
        .when((col("voter_age") >= 45) & (col("voter_age") < 55), "45-54")
        .when((col("voter_age") >= 55) & (col("voter_age") < 65), "55-64")
        .otherwise("65+")
    )
    
    # Aggregation 1: Votes per candidate with watermark
    votes_per_candidate = (votes_df
        .withWatermark("voting_time", "10 seconds")
        .groupBy(
            "candidate_id", 
            "candidate_name", 
            "party_affiliation", 
            "photo_url"
        )
        .agg(_sum("vote").alias("total_votes"))
    )
    
    # Aggregation 2: Turnout by location
    turnout_by_location = (votes_df
        .withWatermark("voting_time", "10 seconds")
        .groupBy(col("address.state").alias("state"))
        .agg(count("*").alias("total_votes"))
    )
    
    # Aggregation 3: Votes by age group
    votes_by_age_group = (votes_df
        .withWatermark("voting_time", "10 seconds")
        .groupBy("age_group")
        .agg(
            count("*").alias("total_voters"),
            _sum("vote").alias("total_votes")
        )
    )
    
    # Aggregation 4: Votes by gender
    votes_by_gender = (votes_df
        .withWatermark("voting_time", "10 seconds")
        .groupBy("gender", "party_affiliation")
        .agg(count("*").alias("total_votes"))
    )

    # Write to console for debugging
    console_query = (votes_per_candidate
        .writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .trigger(processingTime="2 seconds")
        .start()
    )
    
    # Write aggregations to Kafka with proper serialization
    votes_per_candidate_to_kafka = (votes_per_candidate
        .select(to_json(struct("candidate_id", "candidate_name", "party_affiliation", "photo_url", "total_votes")).alias("value"))
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("topic", "aggregated_votes_per_candidate")
        .option("checkpointLocation", "./checkpoints/checkpoint_candidate")
        .option("failOnDataLoss", "false")
        .outputMode("update")
        .trigger(processingTime="5 seconds")
        .start()
    )
    
    turnout_by_location_to_kafka = (turnout_by_location
        .select(to_json(struct("state", "total_votes")).alias("value"))
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("topic", "aggregated_turnout_by_location")
        .option("checkpointLocation", "./checkpoints/checkpoint_location")
        .option("failOnDataLoss", "false")
        .outputMode("update")
        .trigger(processingTime="5 seconds")
        .start()
    )
    
    votes_by_age_to_kafka = (votes_by_age_group
        .select(to_json(struct("age_group", "total_voters", "total_votes")).alias("value"))
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("topic", "aggregated_votes_by_age")
        .option("checkpointLocation", "./checkpoints/checkpoint_age")
        .option("failOnDataLoss", "false")
        .outputMode("update")
        .trigger(processingTime="5 seconds")
        .start()
    )
    
    votes_by_gender_to_kafka = (votes_by_gender
        .select(to_json(struct("gender", "party_affiliation", "total_votes")).alias("value"))
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("topic", "aggregated_votes_by_gender")
        .option("checkpointLocation", "./checkpoints/checkpoint_gender")
        .option("failOnDataLoss", "false")
        .outputMode("update")
        .trigger(processingTime="5 seconds")
        .start()
    )
    
    print("Spark Streaming started. Waiting for data...")
    spark.streams.awaitAnyTermination()