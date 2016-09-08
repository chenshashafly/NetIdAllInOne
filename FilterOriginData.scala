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
//先得出mac上的userid的数目的分布，用R分析找到临界点，以临界点来判断mac是否为NAT。
//同理，得出userid出现在不同的mac上的数目分布，得到临界点，来判断userid是否为”无效id“，所谓无效id是对目前身份合一的研究没有用的数据id。
object FilterOriginData {
    def main(args:Array[String])={
      val conf=new SparkConf().setAppName("filterOriginData")//.setMaster("local")
     // val conf=new SparkConf().setAppName("groupid_v4")
      val sc=new SparkContext(conf)
      
      //im分析, 第一轮基本过滤
      //val im = sc.textFile("/home/chensha/stephanie/input_data/im-data/im_data/*")
      val im=sc.textFile(args(0))
      val filterIm = im.filter { x =>
       {
        var result = false
        val parts = x.split("\t")
        if (parts.length == 19 || parts.length == 22) { //两种长度19/22的字段，8号1002为QQ记录，1009为手机号码；
          if (parts(7).equals("1002")) {   
            if (parts(10).length() >= 5 || parts(10).length() <= 11) {              //QQ号长度为5到11，
              val regEx = "[0-9]+".r //正则表达式表示QQ号
              val someQQ = regEx.findFirstIn(parts(10)).toString  //findFirstIn找到字符串中的首个匹配项,即只匹配第一个符合正则的字符串就停止了。
              if (someQQ.length() >= 11) {
                if (someQQ.substring(5, someQQ.length() - 1).equals(parts(10))) { //?为什么不加这个regex会出错呢？
                  if (parts(4).length() <= 10 && parts(0).length() <= 10) { //qztid和time字段长度不超过10
                    if (parts(4).startsWith("1")) { //对时间字段的过滤
                      result = true
                }}
                }
              }
            }
          }
        }
        result
       }
      }
       val keyIm=filterIm.map { x =>  //0/5/6/4/7/10号字段分别表示qztid/ip/mac/time/type(1002/1009分别代表pc/手机端)/qqaccount
      {
        val parts = x.split("\t")
        val time = parts(4).toLong  //如：time=1444579324；day=1444579200
        val day =(time + 60 * 60 * 8) / (60 * 60 * 24)  //8指的是时间起点是从某一天的八点开始的？24是指一天24小时
        val timeleft=(time + 60 * 60 * 8) % (60 * 60 * 24);
        val hour=timeleft/(60*60)  //以一天24小时为单位，分别计算以1h，连续4h为一个时间段，以及一天day的时间
        parts(0).toInt + "\t" + parts(5) + "\t" + parts(6)+ "\t" + day+ "\t" + parts(7) + "\t" + parts(10) //+ "\tendMark"
      }
    }.distinct
    //keyIm.coalesce(5, false).saveAsTextFile("/home/chensha/stephanie/output_data/filteroriginData-IM") 
    keyIm.coalesce(40,false).saveAsTextFile(args(2))
     //email分析
      //val email = sc.textFile("/home/chensha/stephanie/input_data/email-data/em_data/*")
      val email=sc.textFile(args(1))
       val filteremail=email.filter { x =>
       {
        var result = false
        val parts = x.split("\t")
        if (parts.length == 27 || parts.length == 30) { //长度为27/30，8为actionType
          val actionType = parts(7).toInt
          //0/发,1/收,2/web发
          if (actionType == 0 || actionType == 2) {  //只get发信人的邮箱帐号
            if (parts(4).length() <= 10 && parts(0).length() <= 10) {
              if (parts(4).startsWith("1")) {
                if(!parts(9).toLowerCase().split("@")(0).equals("account"))//时间字段
                result = true
                }
              }
          }
        }
        result
      }}
        
       val keyEmail = filteremail.map { x =>      //0/5/6/4/9号分别代表qztid/ip/mac/time（以day为单位）/邮箱帐号emailaccount
         {
        val parts = x.split("\t")
        val time = parts(4).toLong
        val day =(time + 60 * 60 * 8) / (60 * 60 * 24)  //8指的是时间起点是从某一天的八点开始的？24是指一天24小时
        val timeleft=(time + 60 * 60 * 8) % (60 * 60 * 24);
        val hour=timeleft/(60*60)  //以一天24小时为单位，分别计算以1h，连续4h为一个时间段，以及一天day的时间
        //parts(0).toInt + "\t" + parts(5) + "\t" + parts(6) + "\t" + time+ "\t" + day+ "\t" + hour+ "\t9\t" + parts(9).toLowerCase()  //不加时间的限制可以剔除某些身份出现太多的mac，认为是疑似NAT
         parts(0).toInt + "\t" + parts(5) + "\t" + parts(6)+"\t" + day+"\t9\t" + parts(9).toLowerCase()
        //parts(0).toInt + "\t" + parts(6) + "\t9\t" + parts(9).toLowerCase()
      }
    }.distinct
    	//keyEmail.coalesce(1, false).saveAsTextFile("/home/chensha/stephanie/output_data/filteroriginData-Em")
    keyEmail.coalesce(5, false).saveAsTextFile(args(3))
    	}
}