package qqemail
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._
import java.util.ArrayList
import scala.collection.mutable.ArrayBuffer
object geterridNAT_v1 {
	def main(args:Array[String])={
      //val conf=new SparkConf().setAppName("findInvalIdNat").setMaster("local")
      val conf=new SparkConf().setAppName("filtererrrecord").setMaster("local")
      val sc=new SparkContext(conf)
      val uniondata=sc.textFile("/home/chensha/stephanie/output_data/summary/uniondata_filtersingleid")
      //val uniondata=sc.textFile(args(0))
     
      /*val erridrecord1=uniondata.map(x=>{
         val parts=x.split("\t")
         ((parts(4)+"\t"+parts(5)+"\t"+parts(3),parts(2)))
      }).distinct.reduceByKey(_+";"+_).filter{x=>x._2.split(";").length>=4}.map(x=>(x._1,1))
      val erridrecord2=uniondata.map(x=>{
         val parts=x.split("\t")
         ((parts(4)+"\t"+parts(5)+"\t"+parts(3),parts(0)+"\t"+parts(1)+"\t"+parts(2)))
      }).distinct.reduceByKey(_+";"+_).filter{x=>x._2.split(";").length>=8}.map(x=>(x._1,1))
      val erridrecord=erridrecord1.union(erridrecord2)
      erridrecord.keys.distinct.coalesce(1, false).saveAsTextFile("/home/chensha/stephanie/output_data/20160907/erridrecord")*/
      
      /*val errmacrecord1=uniondata.map(x=>{
        val parts=x.split("\t")
         (parts(0)+"\t"+parts(1)+"\t"+parts(2)+"\t"+parts(3),parts(4)+"\t"+parts(5))
      }).distinct.reduceByKey(_+";"+_).filter{x=>x._2.split(";").length>=5}.map(x=>(x._1,1))
      val errmacrecord2=uniondata.map(x=>{
        val parts=x.split("\t")
         (parts(2)+"\t"+parts(3),parts(4)+"\t"+parts(5))
      }).distinct.reduceByKey(_+";"+_).filter{x=>x._2.split(";").length>=10}.map(x=>(x._1,1))
     errmacrecord1.keys.distinct.coalesce(1, false).saveAsTextFile("/home/chensha/stephanie/output_data/20160907/errmacrecord1")
     errmacrecord2.keys.distinct.coalesce(1, false).saveAsTextFile("/home/chensha/stephanie/output_data/20160907/errmacrecord2")*/
           
      val erridrecord=sc.textFile("/home/chensha/stephanie/output_data/20160907/erridrecord").map(x=>(x,1))
      val errmacrecord1=sc.textFile("/home/chensha/stephanie/output_data/20160907/errmacrecord1").map(x=>(x,1))
     val errmacrecord2=sc.textFile("/home/chensha/stephanie/output_data/20160907/errmacrecord2").map(x=>(x,1))

      /*val filtererrid=uniondata.map(x=>{
        val parts=x.split("\t")
         ((parts(4)+"\t"+parts(5)+"\t"+parts(3),x))
      }).subtractByKey(erridrecord).values
      filtererrid.coalesce(5, false).saveAsTextFile("/home/chensha/stephanie/output_data/20160907/filterErridRecord")*/
      val filtererrid=sc.textFile("/home/chensha/stephanie/output_data/20160907/filterErridRecord")
      val filtererrmac=filtererrid.map(x=>{
        val parts=x.split("\t")
        (parts(2)+"\t"+parts(3),x)
      }).subtractByKey(errmacrecord2).values.map(x=>{
        val parts=x.split("\t")
        (parts(0)+"\t"+parts(1)+"\t"+parts(2)+"\t"+parts(3),x)
      }).subtractByKey(errmacrecord1).values
      filtererrmac.coalesce(5, false).saveAsTextFile("/home/chensha/stephanie/output_data/20160907/filterErrRecord")
     // filtererrmac.coalesce(5, false).saveAsTextFile(args(3))
}
}