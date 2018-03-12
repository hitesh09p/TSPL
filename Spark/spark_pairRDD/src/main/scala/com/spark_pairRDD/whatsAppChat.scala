package com.spark_pairRDD
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import scala.util.matching.Regex
object whatsAppChat {
   def main(args: Array[String]){
    
    val conf = new SparkConf()
               .setMaster("local")
               .setAppName("whatsapp-chat")
               
   val sc = new SparkContext (conf)
  // sc.hadoopConfiguration.set("textinputformat.record.delimiter", "] ")
   val whatsappchat = sc.textFile("C:\\Users\\Administrator\\Desktop\\whatsappchat.txt")
  // whatsappchat.foreach(println)
  
   val filtered = whatsappchat.filter(line=>line.contains("["))
   
   // filtered.foreach(println)
   val split= filtered.map(line=>line.split("\\]").map(_.trim))
 // split.foreach(println)
    val dateRDD = split.filter(line=>line.contains("[")).toString()
    
   //dateRDD.foreach(println)

    val data = split.filter(_(0) != dateRDD(0))
 // data.foreach(println)
   val pairRDD = data.map(splits => dateRDD.zip(splits).toString())
 // pairRDD.foreach(println)
  
 
 /* val  singleReg =  """([)(\\d(?:\\d)?:\\d{2} [AP]M)(,)\\s(\\d{2}/\\d{2}/\\d{4})(])\\s([^:]*):(.*?)(?=\\s*\\d{2}/|$)"""".r
  val newpair= split.filter(x => (singleReg.pattern.matcher(x(1)).matches))
  newpair.foreach(println)*/
   
   
   
   }
}