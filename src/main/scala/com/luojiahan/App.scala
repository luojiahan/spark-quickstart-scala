package com.luojiahan

import org.apache.spark.sql.SparkSession

/**
 * @author ${user.name}
 */
object App {

  private val HDFS_API = "hdfs://hadoop000:9000"
  System.setProperty("HADOOP_USER_NAME", "root")

  def main(args : Array[String]) {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.hadoopConfiguration.set("dfs.client.use.datanode.hostname", "true")
    sc.hadoopConfiguration.set("fs.defaultFS", HDFS_API)

    val col = sc.parallelize(0 to 100 by 5)
    val smp = col.sample(true, 4)
    val colCount = col.count
    val smpCount = smp.count

    println("orig count = " + colCount)
    println("sampled count = " + smpCount)
    sc.stop()
  }

}
