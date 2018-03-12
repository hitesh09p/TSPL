package com.spark_pairRDD
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
object filterWood {
  def main(args: Array[String]){
    
    val conf = new SparkConf()
               .setMaster("local")
               .setAppName("Filter-Wood")
               
   val sc = new SparkContext (conf)
    
   val insurance = sc.textFile("D:\\DataFile\\insurance.csv")
  //insurance.take(1).foreach(println)
    val headerAndRows = insurance.map(line => line.split(",").map(_.trim))
   // headerAndRows.collect().foreach(println)
  val header = headerAndRows.first()
  
 val data = headerAndRows.filter(_(0) != header(0))
 data.foreach(println)
   val pairRDD = data.map(splits => header.zip(splits).toMap)
  // pairRDD.foreach(println)
    
   
  val filterData = pairRDD.filter(map => map ("construction") == "Wood")
   
   filterData.foreach(println)
    
   //----------------------------------------------------------------------------------------------------------------
    
  // val lines = sc.parallelize(List( 1,2,3,3))

//  val sum = lines.reduce((x, y) => x+y).toString()
 // sum.foreach(println)
 // val result = lines.map(x=> x*x)
   //    println(result.count())
  //     println(result.collect())
   // val data = Seq(("a", 3), ("b", 4), ("a", 1))
   //sc.parallelize(data).reduceByKey(_ + _)
 // val pardata = sc.parallelize(data).reduceByKey(_ + _)
     // pardata.foreach(println)
  
    
   // val input = sc.textFile("D:\\DataFile\\insurance.csv")
  //  val newrdd =input.map(x => (x.split(" ")(0), x))
  //  val mapFile= input.map(line => (line,line.length))
  //  mapFile.foreach(println)
    
    
  /*  val rdd1 = sc.parallelize(Seq((1,"jan",2016),(3,"nov",2014),(16,"feb",2014)))
val rdd2 = sc.parallelize(Seq((5,"dec",2014),(1,"jan",2016)))
val rdd3 = sc.parallelize(Seq((6,"dec",2011),(16,"may",2015)))
val rddUnion = rdd1.union(rdd2).union(rdd3)
//rddUnion.foreach(println)
   val common = rdd1.intersection(rdd2)
common.foreach(println) */
    

     /* val words = Array("one","two","three","four")
    val wordRDD = sc.parallelize(words).map(word => (word, 1))
    val wordCountsreducebykey = wordRDD.groupByKey().foreach(println)*/
    
   /* val mydata = sc.textFile("C:\\Users\\Administrator\\Desktop\\purplecow.txt")
 val upper= mydata.map(line => line.toLowerCase())
  upper.foreach(println)*/
  

  }
}