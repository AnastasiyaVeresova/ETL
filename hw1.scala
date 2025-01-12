/*
chcp 65001 && spark-shell -i F:/x/ETL/answer/hw1.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val t1 = System.currentTimeMillis()

// Создание SparkSession
val spark = SparkSession.builder
  .appName("HW1")
  .master("local[*]")
  .getOrCreate()

import spark.implicits._

// Создание исходной таблицы
val data = Seq(
  ("E001", "Alice", "J01", "Chef", "26", "Moscow"),
  ("E001", "Alice", "J02", "Waiter", "26", "Moscow"),
  ("E002", "Bob", "J02", "Waiter", "56", "Perm"),
  ("E002", "Bob", "J03", "Bartender", "56", "Perm"),
  ("E003", "Alice", "J01", "Chef", "56", "Perm")
)

val df = data.toDF("EmployeeID", "Name", "JobID", "JobTitle", "Age", "City")

// Первая нормальная форма (1NF)
println("1NF:")
df.show(false)

// Вторая нормальная форма (2NF)
// Разделение таблицы на две: Employees и Jobs
val employeesDF = df.select("EmployeeID", "Name", "Age", "City").distinct()
val jobsDF = df.select("EmployeeID", "JobID", "JobTitle").distinct()

println("2NF: Employees")
employeesDF.show(false)
println("2NF: Jobs")
jobsDF.show(false)

// Третья нормальная форма (3NF)
// Разделение таблицы на три: Employees, Jobs и JobTitles
val jobTitlesDF = df.select("JobID", "JobTitle").distinct()

println("3NF: Employees")
employeesDF.show(false)
println("3NF: Jobs")
jobsDF.show(false)
println("3NF: JobTitles")
jobTitlesDF.show(false)

// Завершение работы SparkSession
spark.stop()

val s0 = (System.currentTimeMillis() - t1) / 1000
val s = s0 % 60
val m = (s0 / 60) % 60
val h = (s0 / 60 / 60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)
