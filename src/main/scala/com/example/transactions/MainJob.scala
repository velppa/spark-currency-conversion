package com.example.transactions

import com.twitter.scalding.Args

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

object JobConfig {
  val config = ConfigFactory.load
  lazy val sparkMaster = config.getString("MainJob.spark.master")
}

object MainJob extends LazyLogging {

  import JobConfig._

  val transactionSchema = new StructType()
      .add("transaction_id",IntegerType,true)
      .add("customer_id",IntegerType,true)
      .add("merchant_id",IntegerType,true)
      .add("card_id",StringType,true)
      .add("timestamp",IntegerType,true)
      .add("currency",StringType,true)
      .add("amount",DoubleType,true)

  val rateSchema = new StructType()
      .add("currency",StringType,true)
      .add("rate",DoubleType,true)
      .add("created_at",IntegerType,true)
      .add("effective_from",IntegerType,true)
      .add("effective_to",IntegerType,true)

  def main(args: Array[String]): Unit = {
    logger.debug(this.getClass.toString)

    val params = Args(args)
    val inputFile = params.required("input")
    val ratesFile = params.required("rates")

    logger.debug(s"inputFile=$inputFile, ratesFile=$ratesFile")

    val sparkConf = new SparkConf()
      .setMaster(sparkMaster)
      .setAppName(this.getClass.toString)

    implicit val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    try {

      val transactions = spark.read.format("csv")
        .option("header", true)
        .schema(transactionSchema)
        .load(inputFile)

      val rates = spark.read.format("csv")
        .option("header", true)
        .schema(rateSchema)
        .load(ratesFile)

      transactions.show
      rates.show

      val interval = 4 * 7 * 24 * 60 * 60

      transactions
        .withColumn("effective_from", col("timestamp") - col("timestamp") % lit(interval))
        .join(rates, Seq("currency", "effective_from"))
        .withColumn("amount_usd", col("amount")*col("rate"))
        .withColumnRenamed("rate", "fx")
        .withColumnRenamed("effective_from", "fx_from")
        .drop("effective_to", "created_at")
        .show
    }
    finally {
      spark.close()
    }
  }
}
