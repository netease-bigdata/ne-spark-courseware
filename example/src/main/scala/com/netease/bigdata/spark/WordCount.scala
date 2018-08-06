package com.netease.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    require(args.length == 1, "Usage: WordCount <input file>")
    val conf = new SparkConf().setAppName("Word Count").setMaster("local[*]")
    val sparkContext = new SparkContext(conf)
    val textFile = sparkContext.textFile(args(0), 2)
    val words = textFile.flatMap(_.split(" "))
    val ones = words.map((_, 1))
    val counts = ones.reduceByKey(_ + _)
    val res = counts.collect()
    for ((word, count) <- res) {
      println(word + ": " + count)
    }

    sparkContext.stop()
  }

}
