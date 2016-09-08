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
//本步骤的目的是过滤出notsingleidgroup，如果某一行的几个id只出现在这一行，意味着这一行的ids已经确定是关联的了，就没必要进行后续计算，从而减少数据计算量。
object filtersingleidgroup {
   def main(args:Array[String])={
      val conf=new SparkConf().setAppName("getQEwt").setMaster("local")
      val sc=new SparkContext(conf)
      val uniondata=sc.textFile("/home/chensha/stephanie/output_data/filterErrRecord_1").
      filter(x=>{
        var result=true
        if((x.split("\t")(4)=="1002")&&(x.split("\t")(5).length>11)){result=false}
        result
        }).
      map(x=>{ 
        val parts=x.split("\t")
        (parts(0)+"\t"+parts(1)+"\t"+parts(2)+"\t"+parts(3),parts(4)+"\t"+parts(5))
        }).reduceByKey(_+";"+_)
        val idgroup=uniondata.values
      idgroup.sortBy(_.split(";").length,true).coalesce(2, false).saveAsTextFile("/home/chensha/stephanie/output_data/unionidgroup")
      val rdd2=idgroup.distinct.flatMap(x=>{
        var array=new ArrayBuffer[(String,String)]();
        val str=x.split(";")
        val sum=1
        for(i<-0 until str.length){
              array+=((str(i),x+"\t"+sum))
               }
         array
       }).reduceByKey(_+"::"+_).
       filter(_._2.split("::").length>=2).values.
       flatMap(_.split("::")).distinct.map(x=>x.split("\t")(0))
       val singleidgroup=idgroup.subtract(rdd2).distinct
       singleidgroup.sortBy(_.split(";").length,true).coalesce(1, false).saveAsTextFile("/home/chensha/stephanie/output_data/singlerecord_process/singlerecord")
       val notsingleidgroup=idgroup.subtract(singleidgroup)
       notsingleidgroup.sortBy(_.split(";").length,true).coalesce(1, false).saveAsTextFile("/home/chensha/stephanie/output_data/notsinglerecord_process/notsinglerecord")
}
}