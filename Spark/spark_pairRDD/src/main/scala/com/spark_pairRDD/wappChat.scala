package com.spark_pairRDD
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import scala.io.Source
object wappChat {
  def main(args: Array[String]){
    
    val conf = new SparkConf()
               .setMaster("local")
               .setAppName("whatsapp-chat")
               
   val sc = new SparkContext (conf)
  
    val mainrdd = sc.textFile("C:\\Users\\Administrator\\Desktop\\current working\\whatsappchat.txt")
    
  val splitrdd = mainrdd.map(line => line.split(" "))
// splitrdd.foreach(println)
  val date = splitrdd.map( x => (x(0),x(4)))
// date.foreach(println)
 //val newgroup = date.groupByKey().foreach(println)

 
 val count =date.map(x=> (x,1))
  .reduceByKey(_ + _)
 .map{ case ((k, v), cnt) => (k, (v, cnt)) }
  .groupByKey.foreach(println)
 
 ///val count = date.groupByKey().map(t => (t._1, t._2.sum))
 //date.foreach(println)
//val countuser = date.flatMap(_._1).map((_, 1)).reduceByKey(_ + _)
//countuser.foreach(println)
 // val group = date.groupByKey().foreach(println)


//------pair rdd using 1st element as key
  //  val pair = date.map(x => (x.split(",")(0), x(8)))
 // pair.foreach(println)
  
  
  
  
//--------------------------------------------  
 // val date1 = splitrdd.map( x => x(0))
// val user1 = splitrdd.map( x => x(4))
//val pairrdd = date1.map(splits => user1.zip(splits).toMap)


  //val countvalue = date.map(x=>(x,1))
 // countvalue.foreach(println)
    //  val user = splitrdd.map(x=>x(4))
//user.foreach(println)
//   val reduce = countvalue.reduceByKey{case (x, y) => x + y} 
      //  reduce.foreach(println)
   //val pair = date.map(splits => user.zip(splits).toMap)
 // pairs.foreach(println)
 // val group=reduce.groupByKey()
 // group.foreach(println)
  }
}