/*
chcp 65001 && spark-shell -i F:\x\ETL\answer\hw2.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val t1 = System.currentTimeMillis()

if (1 == 1) {
  // Создание SparkSession
  val spark = SparkSession.builder
    .appName("FIFA Data Analysis")
    .getOrCreate()

  // Загрузка данных из CSV файла
  var df1 = spark.read
    .option("delimiter", ",")
    .option("header", "true")
    .csv("F:/x/ETL/answer/fifa_s2.csv")

  // Удаление ненужных колонок
  df1 = df1.drop("Joined", "Contract Valid Until")

  // Удаление строк с недостающими значениями
  df1 = df1.na.drop()

  // Приведение значений к единому регистру
  df1 = df1.withColumn("Club", lower(col("Club")))

  // Удаление дубликатов
  df1 = df1.dropDuplicates()

  // Добавление новой колонки с группами возраста
  val ageGroupUDF = udf((age: Int) => {
    age match {
      case a if a < 20 => "Under 20"
      case a if a >= 20 && a < 30 => "20-29"
      case a if a >= 30 && a < 36 => "30-35"
      case a if a >= 36 => "36 and above"
    }
  })

  df1 = df1.withColumn("AgeGroup", ageGroupUDF(col("Age")))

  // Подсчет количества футболистов в каждой категории
  val ageGroupCounts = df1.groupBy("AgeGroup").count()

  // Показать результат
  ageGroupCounts.show()

  // Сохранение результатов в базу данных MySQL
  df1.write.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=123567")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "fifa_data")
    .mode("overwrite")
    .save()

  // Показать первые несколько строк данных
  df1.show(5)

  // Проверка наличия пустых значений
  val s = df1.columns.map(c => sum(col(c).isNull.cast("integer")).alias(c))
  val df2 = df1.agg(s.head, s.tail: _*)
  val t = df2.columns.map(c => df2.select(lit(c).alias("col_name"), col(c).alias("null_count")))
  val df_agg_col = t.reduce((df1, df2) => df1.union(df2))
  df_agg_col.show()
}

val s0 = (System.currentTimeMillis() - t1) / 1000
val s = s0 % 60
val m = (s0 / 60) % 60
val h = (s0 / 60 / 60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)
