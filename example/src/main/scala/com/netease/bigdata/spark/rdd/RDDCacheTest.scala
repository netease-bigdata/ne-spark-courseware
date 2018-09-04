package com.netease.bigdata.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object RDDCacheTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)
      .set("spark.cleaner.periodicGC.interval", "1min") // context cleaner
    val sc = new SparkContext(conf)
    val data = Seq.fill(1024 * 1024 * 100)(Random.nextInt(100))
    val rdd1 = sc.parallelize(data, 20)
    rdd1.cache() // mark rdd 1 cache
    val rdd2 = rdd1.map((_, 1)).reduceByKey(_ + _) // word count
    val cachedRdd2 = rdd2.cache() // cache shuffled rdd
    rdd2.collect() // action actually trigger caching
    rdd1.count()  // ditto
    rdd2.count() // rdd reuse
    cachedRdd2.count() // ditto
    rdd1.map((_, 1)).reduceByKey(_ + _).take(1) // rdd 1 reuse, not rdd 2
    // no rdd reuse
    val rdd3 = sc.parallelize(data, 30)
    rdd3.map((_, 1)).reduceByKey(_ + _).count()
    10.to(20, 2).foreach { i =>
      val tmp = rdd3.groupBy(_ % i)
      tmp.cache().count()
      if (i % 3 == 0) tmp.take(1)
    }
    Thread.sleep(1000 * 60 * 10)
    sc.stop()
  }
}
