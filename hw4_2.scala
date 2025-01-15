//spark-shell -i hw4.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=UTF-8"


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// Create Spark session
val spark = SparkSession.builder
  .appName("Movies Partitioning")
  .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8")
  .getOrCreate()

// Sample data
val data = Seq(
  ("Action", "Director1", 1985, 120, 7.5),
  ("Drama", "Director2", 1995, 100, 8.0),
  ("Comedy", "Director3", 2005, 90, 6.5),
  ("Sci-Fi", "Director4", 2015, 140, 9.0),
  ("Horror", "Director5", 2021, 110, 7.0),
  ("Action", "Director6", 2022, 130, 8.5),
  ("Drama", "Director7", 2023, 150, 9.5),
  ("Comedy", "Director8", 2024, 80, 6.0),
  ("Sci-Fi", "Director9", 2025, 160, 10.5),
  ("Horror", "Director10", 2026, 105, 11.0)
)

val columns = Seq("movies_type", "director", "year_of_issue", "length_in_minutes", "rate")

// Create DataFrame
val df = spark.createDataFrame(spark.sparkContext.parallelize(data)).toDF(columns: _*)

// Write DataFrame to MySQL
df.write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "movies")
  .mode("overwrite")
  .save()

// Partition by year of issue
val df_before_1990 = df.filter(col("year_of_issue") < 1990)
val df_1990_2000 = df.filter((col("year_of_issue") >= 1990) & (col("year_of_issue") < 2000))
val df_2000_2010 = df.filter((col("year_of_issue") >= 2000) & (col("year_of_issue") < 2010))
val df_2010_2020 = df.filter((col("year_of_issue") >= 2010) & (col("year_of_issue") < 2020))
val df_after_2020 = df.filter(col("year_of_issue") >= 2020)

// Write partitioned DataFrames to MySQL
df_before_1990.write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "movies_before_1990")
  .mode("overwrite")
  .save()

df_1990_2000.write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "movies_1990_2000")
  .mode("overwrite")
  .save()

df_2000_2010.write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "movies_2000_2010")
  .mode("overwrite")
  .save()

df_2010_2020.write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "movies_2010_2020")
  .mode("overwrite")
  .save()

df_after_2020.write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "movies_after_2020")
  .mode("overwrite")
  .save()

// Partition by length in minutes
val df_less_than_40 = df.filter(col("length_in_minutes") < 40)
val df_40_to_90 = df.filter((col("length_in_minutes") >= 40) & (col("length_in_minutes") < 90))
val df_90_to_130 = df.filter((col("length_in_minutes") >= 90) & (col("length_in_minutes") < 130))
val df_more_than_130 = df.filter(col("length_in_minutes") >= 130)

// Write partitioned DataFrames to MySQL
df_less_than_40.write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "movies_less_than_40")
  .mode("overwrite")
  .save()

df_40_to_90.write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "movies_40_to_90")
  .mode("overwrite")
  .save()

df_90_to_130.write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "movies_90_to_130")
  .mode("overwrite")
  .save()

df_more_than_130.write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "movies_more_than_130")
  .mode("overwrite")
  .save()

// Partition by rate
val df_less_than_5 = df.filter(col("rate") < 5)
val df_5_to_8 = df.filter((col("rate") >= 5) & (col("rate") < 8))
val df_8_to_10 = df.filter((col("rate") >= 8) & (col("rate") <= 10))
val df_more_than_10 = df.filter(col("rate") > 10)

// Write partitioned DataFrames to MySQL
df_less_than_5.write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "movies_less_than_5")
  .mode("overwrite")
  .save()

df_5_to_8.write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "movies_5_to_8")
  .mode("overwrite")
  .save()

df_8_to_10.write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "movies_8_to_10")
  .mode("overwrite")
  .save()

df_more_than_10.write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "movies_more_than_10")
  .mode("overwrite")
  .save()

// Add movies to each partitioned table
val new_movies = Seq(
  ("Action", "Director11", 1988, 110, 4.5),
  ("Drama", "Director12", 1998, 85, 7.0),
  ("Comedy", "Director13", 2008, 95, 6.0),
  ("Sci-Fi", "Director14", 2018, 125, 8.5),
  ("Horror", "Director15", 2022, 100, 9.5),
  ("Action", "Director16", 2023, 135, 10.5),
  ("Drama", "Director17", 2024, 155, 11.0)
)

val new_df = spark.createDataFrame(spark.sparkContext.parallelize(new_movies)).toDF(columns: _*)

// Insert into partitioned tables
new_df.write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "movies")
  .mode("append")
  .save()

new_df.filter(col("year_of_issue") < 1990).write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "movies_before_1990")
  .mode("append")
  .save()

new_df.filter((col("year_of_issue") >= 1990) & (col("year_of_issue") < 2000)).write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "movies_1990_2000")
  .mode("append")
  .save()

new_df.filter((col("year_of_issue") >= 2000) & (col("year_of_issue") < 2010)).write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "movies_2000_2010")
  .mode("append")
  .save()

new_df.filter((col("year_of_issue") >= 2010) & (col("year_of_issue") < 2020)).write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "movies_2010_2020")
  .mode("append")
  .save()

new_df.filter(col("year_of_issue") >= 2020).write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "movies_after_2020")
  .mode("append")
  .save()

new_df.filter(col("length_in_minutes") < 40).write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "movies_less_than_40")
  .mode("append")
  .save()

new_df.filter((col("length_in_minutes") >= 40) & (col("length_in_minutes") < 90)).write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "movies_40_to_90")
  .mode("append")
  .save()

new_df.filter((col("length_in_minutes") >= 90) & (col("length_in_minutes") < 130)).write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "movies_90_to_130")
  .mode("append")
  .save()

new_df.filter(col("length_in_minutes") >= 130).write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "movies_more_than_130")
  .mode("append")
  .save()

new_df.filter(col("rate") < 5).write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "movies_less_than_5")
  .mode("append")
  .save()

new_df.filter((col("rate") >= 5) & (col("rate") < 8)).write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "movies_5_to_8")
  .mode("append")
  .save()

new_df.filter((col("rate") >= 8) & (col("rate") <= 10)).write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "movies_8_to_10")
  .mode("append")
  .save()

new_df.filter(col("rate") > 10).write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "movies_more_than_10")
  .mode("append")
  .save()

// Add movies with rating higher than 10
val high_rated_movies = Seq(
  ("Action", "Director18", 2025, 140, 10.5),
  ("Drama", "Director19", 2026, 150, 11.0)
)

val high_rated_df = spark.createDataFrame(spark.sparkContext.parallelize(high_rated_movies)).toDF(columns: _*)

// Insert into partitioned tables
high_rated_df.write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "movies")
  .mode("append")
  .save()

high_rated_df.filter(col("year_of_issue") >= 2020).write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "movies_after_2020")
  .mode("append")
  .save()

high_rated_df.filter(col("length_in_minutes") >= 130).write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "movies_more_than_130")
  .mode("append")
  .save()

high_rated_df.filter(col("rate") > 10).write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "movies_more_than_10")
  .mode("append")
  .save()

// Read from all partitioned tables
val df_all = spark.read
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "(SELECT * FROM movies UNION ALL SELECT * FROM movies_before_1990 UNION ALL SELECT * FROM movies_1990_2000 UNION ALL SELECT * FROM movies_2000_2010 UNION ALL SELECT * FROM movies_2010_2020 UNION ALL SELECT * FROM movies_after_2020 UNION ALL SELECT * FROM movies_less_than_40 UNION ALL SELECT * FROM movies_40_to_90 UNION ALL SELECT * FROM movies_90_to_130 UNION ALL SELECT * FROM movies_more_than_130 UNION ALL SELECT * FROM movies_less_than_5 UNION ALL SELECT * FROM movies_5_to_8 UNION ALL SELECT * FROM movies_8_to_10 UNION ALL SELECT * FROM movies_more_than_10) AS combined")
  .load()

df_all.show()

// Read from the main table
val df_main = spark.read
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "movies")
  .load()

df_main.show()
