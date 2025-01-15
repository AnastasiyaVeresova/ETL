/*
chcp 65001 && spark-shell -i "F:\x\ETL\answer\hw3.scala" --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/


import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

val t1 = System.currentTimeMillis()

if (1 == 1) {

  var dfl = spark.read.format("com.crealytics.spark.excel")
    .option("sheetName", "Sheet1")
    .option("useHeader", "false")
    .option("treatEmptyValuesAsNulls", "false")
    .option("inferSchema", "true")
    .option("usePlainNumberFormat", "true")
    .option("startColumn", 0)
    .option("endColumn", 99)
    .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")
    .option("maxRowsInMemory", 20)
    .option("excerptSize", 10)
    .option("header", "true")
    .format("excel")
    .load("F:/x/ETL/info/s3.xlsx")

  dfl.write.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "sem03_table01")
    .mode("overwrite")
    .save()

  val df = spark.read.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "sem03_table01")
    .load()

  val windowSpec = Window.partitionBy("ID_Тикета").orderBy("Status_Time")

  val dfWithStatus = df.withColumn("Status_Time", from_unixtime(col("Status_Time")))
    .withColumn("Длительность", (lead("Status_Time", 1).over(windowSpec).cast("long") - col("Status_Time").cast("long")) / 3600)
    .withColumn("Статус", when(col("Статус").isNull, lit("")).otherwise(col("Статус")))
    .withColumn("Группа", when(col("Группа").isNull, lit("")).otherwise(col("Группа")))
    .withColumn("Назначение", when(col("Назначение").isNull, lit("")).otherwise(col("Назначение")))

  val dfWithShortStatus = dfWithStatus.withColumn("Статус",
    when(col("Статус") === "Зарегистрирован", "З")
      .when(col("Статус") === "Закрыт", "ЗТ")
      .otherwise(col("Статус"))
  )

  val dfWithFormattedTime = dfWithShortStatus.withColumn("Status_Time", date_format(col("Status_Time"), "dd.MM.yy HH.mm"))

  val dfGrouped = dfWithFormattedTime.groupBy("ID_Тикета")
    .agg(concat_ws(", ", collect_list(concat(col("Status_Time"), lit(" "), col("Статус")))).alias("Status_List"))

  dfGrouped.write.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "sem03_table03")
    .mode("overwrite")
    .save()

  val s0 = (System.currentTimeMillis() - t1) / 1000
  val s = s0 % 60
  val m = (s0 / 60) % 60
  val h = (s0 / 60 / 60) % 24
  println("%02d:%02d:%02d".format(h, m, s))
  System.exit(0)
}
