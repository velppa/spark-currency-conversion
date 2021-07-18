
RATES=rates.csv
load-rates:
	clj -X rates/-main :filename '"$(RATES)"'


INPUT=transactions.csv
OUTPUT_DIR=results/batch
run-batch:
	rm -rf $(OUTPUT_DIR)
	spark-submit \
	  --class com.example.transactions.TransactionEnricherBatch \
	  target/scala-2.12/spark-currency-conversion-assembly-0.1.jar \
	  --input $(INPUT) \
	  --rates $(RATES) \
	  --output-dir $(OUTPUT_DIR)


INPUT_DIR=stream
run-stream:
	mkdir -p $(INPUT_DIR)
	rm -rf $(OUTPUT_DIR)
	spark-submit \
	  --class com.example.transactions.TransactionEnricherStream \
	  target/scala-2.12/spark-currency-conversion-assembly-0.1.jar \
	  --input-dir $(INPUT_DIR) \
	  --rates $(RATES) \
	  --output-dir $(OUTPUT_DIR)
