import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Create Spark session
val spark = SparkSession.builder
  .appName("Credit Analysis")
  .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8")
  .getOrCreate()

// Read Excel file
val df1 = spark.read
  .format("com.crealytics.spark.excel")
  .option("sheetName", "Sheet1")
  .option("useHeader", "true")
  .option("treatEmptyValuesAsNulls", "false")
  .option("inferSchema", "true")
  .option("addColorColumns", "true")
  .option("usePlainNumberFormat", "true")
  .option("startColumn", 0)
  .option("endColumn", 99)
  .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")
  .option("maxRowsInMemory", 20)
  .option("excerptSize", 10)
  .option("header", "true")
  .load("F:/x/ETL/info/s4_2.xlsx")

// Write DataFrame to MySQL
df1.write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "s4_2")
  .mode("overwrite")
  .save()
