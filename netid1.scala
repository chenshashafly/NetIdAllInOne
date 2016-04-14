package learnspark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.JavaConversions._
import utils.netidManager
import utils.netidPara
import java.util.ArrayList
import scala.collection.mutable.ArrayBuffer

object NetIdAllInOne {
       def main(args:Array[String])={
           val nim=new netidManager()
         //  val conf=new SparkConf().setAppName("netidstep1bak").setMaster("local")
           val conf=new SparkConf().setAppName("NetAllInOne")
       //改进一：I/O制度中的压缩配置方式，节约空间，获得时间上效率的提高
          conf.getBoolean("spark.broadcast.compress", true)
           conf.set("spark.broadcast.compress", "true")
           
           val sc=new SparkContext(conf)
          // val textFile=sc.textFile("/home/chensha/stephanie/output_data/NetIdAllInOne/testData")
           val textFile=sc.textFile(args(0),250) //针对数据量为37.6GB的数据，分区为200或者250即可

           val iterations=5
           val step1rdd1=textFile.map(
                line=>{
                 var key="";
                 var value="";
                 if(nim.isLegalData(line)){
                    key=nim.getStrQztId + "," + nim.getStrMac + "," + nim.getHourTime;
                    value=nim.getStrNetIdType + "," + nim.getStrNetId;
                  }
                   (key,value)
               }).partitionBy(new org.apache.spark.HashPartitioner(200)).reduceByKey(_+";"+_).cache
            //改进二：partitionBy函数，参见pageRank的改进方法而来   
           val step1rdd2=textFile.map(
             line=>{
               var key=""
               var value="";
               if(nim.isLegalData(line)){
                 key=nim.getStrNetIdType()+","+nim.getStrNetId();
                 value=nim.getStrMac();
               }
                 (key,value)
           }).partitionBy(new org.apache.spark.HashPartitioner(200)).cache
           val step1rdd3=textFile.map(
             line=>{
               var key="";
               var value="";
               if(nim.isLegalData(line)){
                 key=nim.getStrMac();
                 value=line;
               }
                 (key,value)
           }).partitionBy(new org.apache.spark.HashPartitioner(200)).cache
           //过滤出虚拟身份数在2-40的行
          val step2rdd1=step1rdd1.filter(line=>{val lines=line._2.split(";").distinct;lines.length>=2&&lines.length<=40})
      
          
          //计算每个身份的总出现次数，即输出netIdType,netId count
          val getvalues=step2rdd1.values
          val countone2=getvalues.flatMap(_.split(";").distinct).map(word=>(word,1)).reduceByKey(_+_)
         
          //计算身份对出现的次数,即输出netIdType,netId1 netIdType,netId2 count
          val countpairs=getvalues.map(_.split(";").distinct).
                         map{x=>{
                               for(i<-0 until x.length;from=i+1;j<-from until x.length if(x(i).compareTo(x(j))<0))
                                       yield (x(i)+";"+x(j))
                                  for(i<-0 until x.length;from=i+1;j<-from until x.length if (x(i).compareTo(x(j))<0))  
                                       yield (x(j)+";"+x(i))
                                                    }
                                   }.flatMap(_.toList).map(x=>(x,1)).reduceByKey(_+_)

           //并入netIdType,netId1 countid1                       
          val stp4rdd3=countpairs.keyBy(_._1.split(";").apply(0)).join(countone2).values
        
          //并入netIdType,netId2 countid2，写法稍微改变但是效果一致
          val stp5rdd1=stp4rdd3.keyBy(x=>{
                val arr=x._1._1.split(";")
                if(arr.length==2) arr(1)
                else null})//??
          val stp5rdd3=stp5rdd1.join(countone2).values 
          //stp5rdd3.saveAsTextFile("/home/chensha/stephanie/output_data/NetIdAllInOne/2.1")
          stp5rdd3.saveAsTextFile(args(1))

          //输出形式为(((netidtype,netid1;netidtype,netid2 pairscount), netid1count), netid2count)例如(((101,429196908;101,1670241331,6),7),1)
          
          //将符合条件的身份对提取出来
         val stp6r1=stp5rdd3.filter(x=>x._1._2>=5&&x._2>=5)
         val stp6rdd1=stp6r1.map(x=>{
              val id1id2=x._1._1._1.split(";")
              val numdouble=x._1._1._2.toDouble
              val numfirst=x._1._2.toDouble
              val numsecd=x._2.toDouble
              if(numdouble/numfirst>=0.6&&numdouble/numsecd>=0.6)
                   {
                 if(id1id2(0).compareTo(id1id2(1))<0) (id1id2(0)+";"+id1id2(1),2)
                 else (id1id2(1)+";"+id1id2(0),2) //本步将字符串顺序定义好为前小后大
                }
              else {if(numdouble/numfirst>=0.6&&numdouble/numsecd<0.6)
                    {
                    (id1id2(0)+";"+id1id2(1),1)
                    }
                 else{
                   if(numdouble/numfirst<0.6&&numdouble/numsecd>=0.6)
                   {(id1id2(1)+";"+id1id2(0),1)}
                   else {(id1id2(0)+"::"+id1id2(0),0)}
                   }}})
          val stp6rdd2=stp6rdd1.filter(x=>x._1.split("::").length==1)
           
            //将能相互合一的形式合并成一个，即有A B;B A这种形式输出为A B的形式。 
          val stp7rdd2=stp6rdd2.filter(_._2==2).keys
                      .map(x=>
                        {val strings=x.split(";");
                        (1,strings(0)+" "+strings(1))
                        }).partitionBy(new org.apache.spark.HashPartitioner(200)).values.cache
           
         //循环5次身份关联结果收敛
           var stp8rdd1=stp7rdd2
           var linestring:Array[String]=Array()
           for(i<-1 to iterations){
               val rddmap_1temp=stp8rdd1.flatMap(line=>{
               var array=new ArrayBuffer[(String,String)]();
               val str=line.split(" ");
               for(i<-0 until str.length){
                 array+=((str(i),line))
                    }
                  array
             })
           stp8rdd1=rddmap_1temp.reduceByKey(_+" "+_).mapValues(_.split(" ").distinct.mkString(" ")).values
           }
         val stp8red1=stp8rdd1.toArray.distinct
         val stp8result=sc.makeRDD(stp8red1)

          // 输入:原始数据和step8的数据
          // 找出结果中虚拟身份数大于20的虚拟身份对应的MAC,疑似NAT;
          // 输出：MAC   
           val stp9rdd1=stp8result.filter(_.split(" ").length>=20)
          // stp9rdd1.saveAsTextFile("/home/chensha/stephanie/output_data/netids1.3.21")
           val stp9rdd2=stp9rdd1.flatMap(line=>{
                val x=line.split(" ");
             var array=new ArrayBuffer[(String,Int)]();
             for(i<-0 until x.length){
                   array+=((x(i),1))
             }
              array
           })
           val stp9rdd3=stp9rdd2.join(step1rdd2)
           val stp9rdd4=stp9rdd3.map(line=>(line._2._2))
           //  输入：step9的输出数据（MAC）和原始数据
           // 在原始数据中剔除step9中出现的MAC的记录; 
           // 输出：少了疑似NAT记录的类原始数据
           val stp10rdd1=stp9rdd4.toArray.distinct //输出去重后的疑似NAT
           val stp10rdd2=step1rdd3.filter(line=>stp10rdd1.contains(line._1))
           val stp10rdd3=step1rdd3.subtract(stp10rdd2).values
          // stp10rdd3.saveAsTextFile("/home/chensha/stephanie/output_data/NetIdAllInOne/2.2")
            stp10rdd3.saveAsTextFile(args(2))

     }
}
