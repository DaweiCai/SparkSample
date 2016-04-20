package org.test.helloword.spark

import org.apache.spark.SparkConf
//import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]) {
    //    val input = args(0)
    //    val conf = new SparkConf().setAppName("Spark count THE from doc")
    //    val sc = new SparkContext(conf)
    //    val sqlc = new SQLContext(sc)
    //    val peopleList = sqlc.read.json(input)
    //    peopleList.filter(peopleList("age") > 21).write.save(args(1))

    //local test
    System.setProperty("hadoop.home.dir", "E:\\Java\\workspace\\winutil\\")
    
    val conf = new SparkConf().setAppName("SparkWordCount").setMaster("local")
    val sc = new SparkContext(conf)

    
    val test = sc.textFile("Name.txt")
    test.flatMap { line => line.split(" ") }.map { word => (word, 1) }.reduceByKey(_ + _).saveAsTextFile("NameCount.txt")

  }
}