package qqemail

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.JavaConversions._
import java.util.ArrayList
import scala.collection.mutable.ArrayBuffer
import scala.util.matching._
import org.apache.spark.HashPartitioner
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.GraphOps
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.lib.ConnectedComponents
//V1是以(qztid ip mac)为key值，但是在计算每个id出现在哪些key值上时，发现绝大部分的qztid+mac是相同的，只有ip不同，所以ip是否需要出现在这个key值里？
//V2是以（qztid mac）为key值来计算的。
//少了ip字段的限制，预计NAT数目会变多，而invalid数目则会变少，（如一个id本来出现在4个(qztid ip mac)上，却只出现在2个（qztid mac）上），
//使得有些id不能被计入无效名单中。
object CCresult {
    def main(args:Array[String])={
      val conf=new SparkConf().setAppName("ccresult").setMaster("local")
      //val conf=new SparkConf().setAppName("CCresult")
      val sc=new SparkContext(conf)
      //val ids=sc.textFile("/home/chensha/stephanie/output_data/filtererridNATMethod/changefilterids")
      //val ids=sc.textFile("/home/chensha/stephanie/learn-MLlib/ids")
      //val ids=sc.textFile("/home/chensha/stephanie/learn-MLlib/uniontwo")
      //val ids=sc.textFile("/home/chensha/stephanie/output_data/summary/finalpair")
      val ids=sc.textFile("/home/chensha/stephanie/output_data/20160908/finalpair_m4")
      // 过滤id对为邮箱帐号且后缀相同的pair。
     val filterids=ids.filter(x=>{
        var result=true
        if(x.split(";")(0).split("\t")(0)=="9"&&x.split(";")(1).split("\t")(0)=="9"){
        val emailname1=x.split(";")(0).split("\t")(1).split("@")(1).split("\\.")(0)
        val emailname2=x.split(";")(1).split("\t")(1).split("@")(1).split("\\.")(0)       
        if((emailname1.equals(emailname2))){result=false}
        }
        result
      })
     //filterids.coalesce(1,false).saveAsTextFile("/home/chensha/stephanie/output_data/notsinglerecord_process/filterpairs_1.3")
     /* val ids=filterids.map(_.split(";")).
                         flatMap(x=>{
          for(i<-0 until x.length;from=i+1;j<-from until x.length if(x(i)!=x(j)))
                 yield (x(i)+";"+x(j))
         // for(i<-0 until x.length;from=i+1;j<-from until x.length if (x(i).compareTo(x(j))>=0))  
               //  yield (x(j)+";"+x(i))
                         }).map(x=>{
       val info=x.split(";")
       if(info(0).compareTo(info(1))>=0){
          info(1)+";"+info(0)
       }else{ info(0)+";"+info(1)}
     }).distinct*/
     var idNum=0L
     val netidNumMap=filterids.map(x=>x.split(";")).flatMap{x=>
       {
           for(e<-x) yield e  
       }}.distinct.map(x=>{
         idNum=idNum+1L;
         (x,idNum)
       }).collectAsMap
       //netidNumMap.foreach(println)
       //val Maprdd=sc.makeRDD(netidNumMap.mkString(";"))
       //Maprdd.saveAsTextFile("/home/chensha/stephanie/output_data/qqemail/netidNumMap")
       val idedgefile=filterids.map(x=>x.split(";")).flatMap{x=>
         {
           for (i <- 1 until x.length) yield (netidNumMap(x(0)) + "\t" + netidNumMap(x(i))) //注意until和to的区别
           }
         }
      // idedgefile.coalesce(1,false).saveAsTextFile("/home/chensha/stephanie/output_data/qqemail/idedgefile4")      
       //idedgefile.coalesce(1,false).saveAsTextFile("/home/chensha/stephanie/output_data/filtererridNATMethod/idedgefile_1")
       idedgefile.coalesce(1,false).saveAsTextFile("/home/chensha/stephanie/output_data/20160908/idedgefile_1.4")
       val useridfile=filterids.map(x=>x.split(";")).flatMap{x=>
       {
           for(e<-x) yield e  
       }}.distinct.map(x=>{
         idNum=idNum+1;
         (x,idNum)
       })
       //useridfile.coalesce(1,false).saveAsTextFile("/home/chensha/stephanie/output_data/qqemail/useridfile4")
      // useridfile.partitionBy(new HashPartitioner(1)).saveAsTextFile("/home/chensha/stephanie/output_data/filtererridNATMethod/useridfile_1")
       useridfile.coalesce(1,false).saveAsTextFile("/home/chensha/stephanie/output_data/20160908/useridfile_1.4")
       val graph=GraphLoader.edgeListFile(sc,"/home/chensha/stephanie/output_data/20160908/idedgefile_1.4")
       val cc=graph.connectedComponents.vertices.mapValues(x=>x.toLong)
       //cc.saveAsTextFile("/home/chensha/stephanie/output_data/qqemail/cc4")
      cc.coalesce(1, false).saveAsTextFile("/home/chensha/stephanie/output_data/20160908/cc__1.4")
       val users=useridfile.map{
         case (key,value) => (value,key)
       }
       val ccByUsername=users.join(cc).map
         {
            case (id, (username, cc)) => (username, cc)
    }
       val finalresult=ccByUsername.map { case (username, userid) => (userid, username) }.reduceByKey(_ + "," + _).values
        //ccByUsername.distinct().saveAsTextFile("/home/chensha/stephanie/output_data/qqemail/ccByUsername4")
        //ccByUsername.distinct().coalesce(1, false).saveAsTextFile( "/home/chensha/stephanie/output_data/filtererridNATMethod/ccbyUsername")
        //finalresult.distinct().saveAsTextFile("/home/chensha/stephanie/output_data/qqemail/finalresult4")
       //finalresult.distinct().coalesce(1, false).saveAsTextFile("/home/chensha/stephanie/output_data/filtererridNATMethod/finalccresult_1")
       finalresult.coalesce(1,false).saveAsTextFile("/home/chensha/stephanie/output_data/20160908/finresult__1.4")
        val ananlysresult=finalresult.map(x=>(x.split(",").length,1)).reduceByKey(_+_).sortByKey(true)
        //ananlysresult.coalesce(1, false).saveAsTextFile("/home/chensha/stephanie/output_data/filtererridNATMethod/analysccres_1_1")
        ananlysresult.coalesce(1,false).saveAsTextFile("/home/chensha/stephanie/output_data/20160908/analysres__1.4")
    	}
   }