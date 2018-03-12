package com.spark_pairRDD
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
object pageRank {
    def main(args: Array[String]) : Unit = {
    
     val conf = new SparkConf()
               .setMaster("local")
               .setAppName("whatsapp-chat")
               
   val sc = new SparkContext (conf)
  val data = sc.textFile("C:\\Users\\Administrator\\Desktop\\pagerank.txt").map(line=>line.split("\\s+")).map( x => (x(0),x(1)) )
    // data.foreach(println)
 val groupdata=data.groupByKey()
  //groupdata.foreach(println)
 val rank = sc.parallelize(List(("www.google.com",1.0),("www.facebook.com",1.0),("www.gamil.com",1.0),("www.yahoo.com",1.0))) 
    // rank.foreach(println)
 val dataJoinRank = groupdata.join(rank)
   // dataJoinRank.foreach(println)
 val dataValue = dataJoinRank.flatMap { case (url, (links, rank)) => links.map(dest => (dest, rank/links.size)) }
    
   dataValue.foreach(println)
    

}
    }