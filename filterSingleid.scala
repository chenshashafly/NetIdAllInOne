package qqemail

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.JavaConversions._
import java.util.ArrayList
import scala.collection.mutable.ArrayBuffer
import scala.util.matching._
import scala.math._
import org.apache.spark.HashPartitioner

object filterSingleid {
    def main(args:Array[String])={
      val conf=new SparkConf().setAppName("getQEwt").setMaster("local")
      val sc=new SparkContext(conf)
     val keyIm=sc.textFile("/home/chensha/stephanie/output_data/summary/imdata")
     val keyEmail=sc.textFile("/home/chensha/stephanie/output_data/summary/emdata")    
      //统计从未与其他id一起出现的id，这种id没有研究的意义。
     val uniondata=keyIm.union(keyEmail)
     //println(uniondata.map(x=>x.split("\t")(5)).distinct.count)
     val singleid= uniondata.map(x=>{(x.split("\t")(2),x.split("\t")(4)+"\t"+x.split("\t")(5))}).distinct.
      reduceByKey(_+";"+_).values.map(_.split(";")).
      flatMap(x=>{
         for(i<-0 until x.length)
                 yield (x(i),x.length)
      }).distinct.reduceByKey(_+_).filter(_._2==1).keys     
      singleid.coalesce(1, false).saveAsTextFile("/home/chensha/stephanie/output_data/summary/singleid1")
   // val singleid=sc.textFile("/home/chensha/stephanie/output_data/summary/singleid").distinct.count
   // println(singleid)
    //val unionimem_filtersingleid=uniondata.map{x=>(x.split("\t")(4)+"\t"+x.split("\t")(5),x)}.subtractByKey(singleid).values
    //unionimem_filtersingleid.coalesce(10, false).saveAsTextFile("/home/chensha/stephanie/output_data/summary/uniondata_filtersingleid")
      //val unionimem_filtersingleid=sc.textFile("/home/chensha/stephanie/output_data/summary/unionimem_filtersingleid").map(x=>x.split("\t")(5))
     // println(unionimem_filtersingleid.distinct.count)
    }
   
}