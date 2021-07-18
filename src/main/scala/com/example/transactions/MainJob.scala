package com.example.transactions

import com.twitter.scalding.Args

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

object Schemas {
  val transaction = new StructType()
       .add("transaction_id",IntegerType,true)
       .add("customer_id",IntegerType,true)
       .add("merchant_id",IntegerType,true)
       .add("card_id",StringType,true)
       .add("timestamp",IntegerType,true)
       .add("currency",StringType,true)
       .add("amount",DoubleType,true)

  val rate = new StructType()
       .add("currency",StringType,true)
       .add("rate",DoubleType,true)
       .add("created_at",IntegerType,true)
       .add("effective_from",IntegerType,true)
       .add("effective_to",IntegerType,true)
}

object Enricher {
  def withAmountUSD(transactions: DataFrame, rates: DataFrame): DataFrame = {
    val interval = 4 * 7 * 24 * 60 * 60

    transactions
     .withColumn("effective_from", col("timestamp") - col("timestamp") % lit(interval))
     .join(rates, Seq("currency", "effective_from"))
     .withColumn("amount_usd", col("amount")*col("rate"))
     .withColumnRenamed("rate", "fx")
     .withColumnRenamed("effective_from", "fx_from")
     .drop("effective_to", "created_at")
  }
}

object TransactionEnricherBatch extends LazyLogging {
  val config = ConfigFactory.load.getConfig("TransactionEnricherBatch")

  def main(args: Array[String]): Unit = {
    val params = Args(args)
    val inputFile = params.required("input")
    val ratesFile = params.required("rates")
    val outputDir = params.required("output-dir")

    logger.debug(s"inputFile=$inputFile, ratesFile=$ratesFile")

    val sparkConf = new SparkConf()
      .setMaster(config.getString("spark.master"))
      .setAppName(this.getClass.toString)

    implicit val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    try {
      val transactions = spark.read.format("csv")
        .option("header", true)
        .schema(Schemas.transaction)
        .load(inputFile)

      val rates = spark.read.format("csv")
        .option("header", false)
        .schema(Schemas.rate)
        .load(ratesFile)

      transactions.show
      rates.show

      Enricher.withAmountUSD(transactions, rates)
        .repartition(1)
        .write
        .option("header", true)
        .csv(outputDir)
    }
    finally {
      spark.close()
    }
  }
}

object TransactionEnricherStream extends LazyLogging {
  val config = ConfigFactory.load.getConfig("TransactionEnricherStream")

  def main(args: Array[String]): Unit = {
    val params = Args(args)
    val transactionsDir = params.required("input-dir")
    val ratesFile = params.required("rates")

    val sparkConf = new SparkConf()
      .setMaster(config.getString("spark.master"))
      .setAppName(this.getClass.toString)

    implicit val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    try {
      val transactions = spark.readStream
        .schema(Schemas.transaction)
        .csv(transactionsDir)

      val rates = spark.read.format("csv")
        .option("header", false)
        .schema(Schemas.rate)
        .load(ratesFile)

      val enriched = Enricher.withAmountUSD(transactions, rates)
        .writeStream
        .format("console")
        .start()

      enriched.awaitTermination()
    }
    finally {
      spark.close()
    }
  }
}
