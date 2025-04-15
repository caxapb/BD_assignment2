import sys
from pyspark.sql import SparkSession

def test_cassandra_connection():
    try:
        # Initialize Spark Session with Cassandra config
        spark = SparkSession.builder \
            .appName("Cassandra Connection Test") \
            .config("spark.cassandra.connection.host", "cassandra-server") \
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0") \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "1g") \
            .getOrCreate()

        print("Attempting to read from Cassandra...")
        
        result = spark.sql("SELECT key, value FROM search_engine.stats LIMIT 1")
        result.show()

        # # Test read from stats table
        # stats_df = spark.read \
        #     .format("org.apache.spark.sql.cassandra") \
        #     .options(table="stats", keyspace="search_engine") \
        #     .load()
        
        # # Show sample data
        # print("\nSample data from stats table:")
        # stats_df.show(5)
        
        # Test count query
        # print("\nTable counts:")
        # print(f"- stats: {stats_df.count()} rows")
        
        # # Test simple query
        # avg_length = stats_df.filter(col("key") == "avg_length") \
        #                    .select("value") \
        #                    .first()[0]
        # print(f"\nAverage document length: {avg_length}")
        
        # spark.stop()
        # print("\n✅ Connection test successful!")
        # return True
        
    except Exception as e:
        print(f"\n❌ Connection failed: {str(e)}")
        if 'spark' in locals():
            spark.stop()
        return False

if __name__ == "__main__":
    test_cassandra_connection()