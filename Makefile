
submit:
	spark-submit \
	  --conf spark.driver.extraClassPath=./  \
      --conf spark.executor.extraClassPath=./ \
	  target/scala-2.12/spark-currency-conversion-assembly-0.1.jar \
	  --input transactions.csv \
	  --rates rates.csv
