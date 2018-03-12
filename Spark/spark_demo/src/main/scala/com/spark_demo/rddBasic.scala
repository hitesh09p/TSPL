package com.spark_demo
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
object rddBasic {
  def main(args: Array[String]){
    
    val conf = new SparkConf()
               .setMaster("local")
               .setAppName("Filter-Wood")
               
   val sc = new SparkContext (conf)
  
     val insurance = sc.textFile("D:\\DataFile\\insurance.csv")
    // insurance.foreach(println)
     
    
    def isHeader(line: String): Boolean = {
line.contains("construction")
}
 // val top10 = insurance.take(10)
   
  val header= insurance.filter(isHeader)  
   val header1 = insurance.filter(line => line.contains("construction")) 
  
  // val nonheader = top10.filter(x => !isHeader(x))
   val Wooddata = insurance.filter(line => line.contains("Wood"))
  // top10.filterNot(isHeader).foreach(println)
  //  top10.filter(!isHeader(_)).foreach(println)
    
  // Wooddata.take(20)

  // val union = header ++ Wooddata
  // union.take(20).foreach(println)

   
   
   val t = (4,3,1,1)
   
   val sum1 = (t._1 + t._2 + t._3 + t._4).toString()
 //  sum1.foreach(println)
   
   
   
   //---------------differnce between map and flatmap-------------
  val data = Array(1,2,3,4)
   
 // val square =sc.parallelize(data).map(x=> (x,x*x)).foreach(println)
  val square1 = sc.parallelize(data).flatMap(x=> List(x, x*x)).foreach(println)

  
  val x = sc.parallelize(List("aa bb cc dd",  "ee ff gg hh"))
//  x.map(x => x.split(" ")).foreach(println)
 // x.flatMap(x => x.split(" "))
  

  
  
  
  
  
  
  
   //-------------difference between reduceByKey & groupByKey--------------
 val data1 = Array("one", "two", "two", "three", "three", "three")
  val sum =  sc.parallelize(data1).map(word => (word, 1))
 // sum.reduceByKey(_ + _)
  sum.groupByKey().map(t => (t._1, t._2.sum))
   //sum.foreach(println)
  
 // sum.reduceByKey((x,y)=> (x+y))
  
  
  
  
  //-----------------------joins--------------------------------------
 val emp = sc.parallelize(Seq((1,"jordan",10), (2,"ricky",20), (3,"matt",30), (4,"mince",35), (5,"rhonda",30)))

val dept = sc.parallelize(Seq(("hadoop",10), ("spark",20), ("hive",30), ("sqoop",40)))

val manipulated_emp = emp.keyBy(t => t._3)

val manipulated_dept = dept.keyBy(t => t._2)

// Inner Join
val join_data = manipulated_emp.join(manipulated_dept)
join_data.foreach(println)
// Left Outer Join
val left_outer_join_data = manipulated_emp.leftOuterJoin(manipulated_dept).foreach(println)
// Right Outer Join
val right_outer_join_data = manipulated_emp.rightOuterJoin(manipulated_dept).foreach(println)
// Full Outer Join
val full_outer_join_data = manipulated_emp.fullOuterJoin(manipulated_dept).foreach(println)
 //  full_outer_join_data.foreach(println)
  
 




//----------------------pairrdd----------------------- 
   val splitrdd =insurance.map(line => line.split(",").map(_.trim))
  val header2= splitrdd.first()
    val datardd =splitrdd.filter(_(0) != header2(0))
     val pairRDD = datardd.map(splits => header2.zip(splits).toMap)
      val filterRdd = pairRDD.filter(map => map ("construction") == "Wood")
    // filterRdd.groupBy(x=>x).foreach(println)
  }
}