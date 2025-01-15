cd F:/x/ETL/answer

@echo off
REM Запуск Scala скрипта
spark-shell -i hw4.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=UTF-8"

REM Запуск Python скрипта
python hw4.py
