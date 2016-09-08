package qqemail
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.JavaConversions._
import java.util.ArrayList
import scala.collection.mutable.ArrayBuffer
import scala.math._
//加上时间字段的day下计算身份对出现的次数和每一个身份的次数，计算两者之比。
//注意，不过滤NAT和errid的话会怎么样呢？
object FindFrequentPair_1 {
	def main(args:Array[String])={
      val conf=new SparkConf().setAppName("ccresult").setMaster("local")
      //val conf=new SparkConf().setAppName("CCresult")
      val sc=new SparkContext(conf)     
      val rightuniondata=sc.textFile("/home/chensha/stephanie/output_data/20160907/filterErrRecord").
        filter(x=>{
           var result=true
           if((x.split("\t")(4)=="1002")&&(x.split("\t")(5).length>11)){result=false}
          result
        }).map(x=>{ 
        val parts=x.split("\t")
        (parts(0)+"\t"+parts(1)+"\t"+parts(2)+"\t"+parts(3),parts(4)+"\t"+parts(5))
        }).reduceByKey(_+";"+_).values
   
   // 接下来得到ids，得到ids数据中的每行中的身份都是可靠关联的，即可以认为是同属于一个人
      //计算每个id出现的次数
   val countone2=rightuniondata.flatMap(_.split(";")).map(word=>(word,1)).reduceByKey(_+_) 
   // countone2.coalesce(1, false).saveAsTextFile("/home/chensha/stephanie/output_data/20160908/countsingle")     
    //计算身份对出现的次数,即输出netIdType,netId1 netIdType,netId2 count
    
    /*val countpairs=rightuniondata.values.filter(_.split(";").length>=2).map(_.split(";")).
                         map{x=>{
                               for(i<-0 until x.length;from=i+1;j<-from until x.length if(x(i).compareTo(x(j))<0))
                                       yield (x(i)+";"+x(j))
                                  for(i<-0 until x.length;from=i+1;j<-from until x.length if (x(i).compareTo(x(j))<0))  
                                       yield (x(j)+";"+x(i))
                                                    }
                                   }.flatMap(_.toList).map(x=>(x,1)).reduceByKey(_+_) //shuffle3,所用写法不对造成shuffle消耗内存大，MRUCwt_v3有改正。*/
     val rdd2=rightuniondata.filter(_.split(";").length>=2).map(_.split(";")).flatMap(x=>{
          for(i<-0 until x.length;from=i+1;j<-from until x.length if(x(i)!=x(j)))
                 yield (x(i)+";"+x(j))
         // for(i<-0 until x.length;from=i+1;j<-from until x.length if (x(i).compareTo(x(j))>=0))  
               //  yield (x(j)+";"+x(i))

     })
     val countpairs=rdd2.map(x=>{
       val info=x.split(";")
       if(info(0).compareTo(info(1))>=0){
          info(1)+";"+info(0)
       }else{ info(0)+";"+info(1)}
     }).map(x=>(x,1)).reduceByKey(_+_)
      //   countpairs.coalesce(1,false).saveAsTextFile("/home/chensha/stephanie/output_data/20160908/countpair")
           //并入netIdType,netId1 countid1                       
          val stp4rdd3=countpairs.keyBy(_._1.split(";")(0)).join(countone2).values //shuffle4
        //stp4rdd3.coalesce(1, false).saveAsTextFile(args(4))
          //并入netIdType,netId2 countid2
          val stp5rdd1=stp4rdd3.keyBy(x=>{ //shuffle5
                val arr=x._1._1.split(";")
                if(arr.length==2) arr(1)
                else null})//??
          val stp5rdd3=stp5rdd1.join(countone2).values 
        //  stp5rdd3.coalesce(1, false).saveAsTextFile("/home/chensha/stephanie/output_data/20160908/countsinglepair")
           //输出形式为(((netidtype,netid1;netidtype,netid2 pairscount), netid1count), netid2count)例如(((101,429196908;101,1670241331,6),7),1)
          //过滤条件1：将符合条件的身份对提取出来：条件是countid12/sqrt(countid1*countid2)>0.7
       /*val judgecond1= stp5rdd3.map(x=>{
              val id1id2=x._1._1._1.split(";")
              val numdouble=x._1._1._2.toDouble
              val numfirst=x._1._2.toDouble
              val numsecd=x._2.toDouble
              val sq=sqrt(numfirst*numsecd)
              if(numdouble/sq>=0.7)
                   {
                 if(id1id2(0).compareTo(id1id2(1))<0) {(id1id2(0)+";"+id1id2(1),2)}
                 else {(id1id2(1)+";"+id1id2(0),2)} //本步将字符串顺序定义好为前da后xiao
                }
              else {(id1id2(0)+";"+id1id2(1),0)}              
         })
         val analysjudgecond1=judgecond1.map(x=>(x._2,1)).reduceByKey(_+_)
          analysjudgecond1.coalesce(1, false).saveAsTextFile("/home/chensha/stephanie/output_data/analysjudgecond_1.1")
         val finalpair1=judgecond1.filter(x=>x._2==2).keys.distinct
        finalpair1.coalesce(1, false).saveAsTextFile("/home/chensha/stephanie/output_data/finalpair_1.1")
        
        //method1_filtercountsinglepair,过滤出符合条件1的身份对的结果
        val method1_filtercountsinglepair=stp5rdd3.filter(x=>{
        		var result=false
              val id1id2=x._1._1._1.split(";")
              val numdouble=x._1._1._2.toDouble
              val numfirst=x._1._2.toDouble
              val numsecd=x._2.toDouble
              val sq=sqrt(numfirst*numsecd)
              if(numdouble/sq>=0.7)
                   {
            	  	result=true
                }
             result              
         })
         method1_filtercountsinglepair.coalesce(1, false).saveAsTextFile("/home/chensha/stephanie/output_data/method1_filtercountsinglepair")*/
        
          //过滤条件2：将符合条件的身份对提取出来,条件是numdouble/numfirst>=0.6&&numdouble/numsecd>=0.6
         //val stp6r1=stp5rdd3.filter(x=>x._1._2>=2&&x._2>=2)
        /* val judgecond2=stp5rdd3.map(x=>{
              val id1id2=x._1._1._1.split(";")
              val numdouble=x._1._1._2.toDouble
              val numfirst=x._1._2.toDouble
              val numsecd=x._2.toDouble
              if(numdouble/numfirst>=0.6&&numdouble/numsecd>=0.6)
                   {
                 if(id1id2(0).compareTo(id1id2(1))<0) (id1id2(0)+";"+id1id2(1),2)
                 else (id1id2(1)+";"+id1id2(0),2) //本步将字符串顺序定义好为前da后xiao
                }
              else {if(numdouble/numfirst>=0.6&&numdouble/numsecd<0.6)
                    {
                    (id1id2(0)+";"+id1id2(1),1)
                    }
                 else{
                   if(numdouble/numfirst<0.6&&numdouble/numsecd>=0.6)
                   {(id1id2(1)+";"+id1id2(0),1)}
                   else {(id1id2(0)+";"+id1id2(0),0)}
                   }}})
           val analysjudgecond2=judgecond2.map(x=>{(x._2,1)}).reduceByKey(_+_)
         analysjudgecond2.coalesce(1, false).saveAsTextFile("/home/chensha/stephanie/output_data/analysjudgecond_1.2")
         val finalpair2=judgecond2.filter(_._2==2).map(x=>x._1).distinct
          finalpair2.coalesce(1, false).saveAsTextFile("/home/chensha/stephanie/output_data/finalpair_1.2")
          
          val method2_filtercountsinglepair=stp5rdd3.filter(x=>{
        		var result=false
              val id1id2=x._1._1._1.split(";")
              val numdouble=x._1._1._2.toDouble
              val numfirst=x._1._2.toDouble
              val numsecd=x._2.toDouble
              if(numdouble/numfirst>=0.6&&numdouble/numsecd>=0.6)
                   {
            	  	result=true
                }
             result              
         })
         method2_filtercountsinglepair.coalesce(1, false).saveAsTextFile("/home/chensha/stephanie/output_data/method2_filtercountsinglepair")*/
         
         //过滤条件3：将符合条件的身份对提取出来,条件是numdouble/numfirst>=0.6||numdouble/numsecd>=0.6 
         val judgecond3=stp5rdd3.map(x=>{
              val id1id2=x._1._1._1.split(";")
              val numdouble=x._1._1._2.toDouble
              val numfirst=x._1._2.toDouble
              val numsecd=x._2.toDouble
              if(numdouble!=1){
              if(numdouble/numfirst>=0.6){
                   if(numdouble/numsecd<=0.1){(id1id2(0)+";"+id1id2(1),0)}
                   else{(id1id2(0)+";"+id1id2(1),1)}       
               }
              else{
              if(numdouble/numfirst<=0.1){
                (id1id2(0)+";"+id1id2(1),0)
                }else{
                  if(numdouble/numsecd>=0.6) {
                     (id1id2(0)+";"+id1id2(1),1)
                    }else{(id1id2(0)+";"+id1id2(1),0)}
                }
              }}
              else{     //对于double只出现一次的情况，只将（1，1，1）（1，1，2）（1，2，1）三种情况作为可靠关联。
               if(numfirst==1){
                      if(numsecd<=2){
                        (id1id2(0)+";"+id1id2(1),1)
                        }
                      else{
                        (id1id2(0)+";"+id1id2(1),0)
                      }
               }
               else{
                   if(numsecd==1){
                        if(numfirst<=2){
                        (id1id2(0)+";"+id1id2(1),1)
                        }
                      else{
                       (id1id2(0)+";"+id1id2(1),0)
                      }
                   }else {(id1id2(0)+";"+id1id2(1),0)}
               }
           }
              })
           val analysjudgecond3=judgecond3.map(x=>{(x._2,1)}).reduceByKey(_+_)
         //analysjudgecond3.coalesce(1, false).saveAsTextFile("/home/chensha/stephanie/output_data/20160908/analysjudgecond_m3")
            //将能相互合一的形式合并成一个，即有A B;B A这种形式输出为A B的形式。 
          val finalpair3_2=judgecond3.filter(_._2==1).map(x=>x._1).distinct
         // finalpair3_2.coalesce(1, false).saveAsTextFile("/home/chensha/stephanie/output_data/20160908/finalpair_m3")
            
          val method3_filtercountsinglepair=stp5rdd3.filter(x=>{
        		var result=true
              val id1id2=x._1._1._1.split(";")
              val numdouble=x._1._1._2.toDouble
              val numfirst=x._1._2.toDouble
              val numsecd=x._2.toDouble
              if(numdouble!=1){
              if(numdouble/numfirst>=0.6){
                   if(numdouble/numsecd<=0.1){result=false}
                   else{result=true}       
               }
              else{
              if(numdouble/numfirst<=0.1){
                result=false
                }else{
                  if(numdouble/numsecd>=0.6) {
                     result=true
                    }else{result=false}
                }
              }
            }
           else{     //对于double只出现一次的情况，只将（1，1，1）（1，1，2）（1，2，1）三种情况作为可靠关联。
               if(numfirst==1){
                      if(numsecd<=2){
                        result=true
                        }
                      else{
                        result=false
                      }
               }
               else{
                   if(numsecd==1){
                        if(numfirst<=2){
                        result=true
                        }
                      else{
                        result=false
                      }
                   }else {result=false}
               }
           }
             result              
         })
         
     //  method3_filtercountsinglepair.coalesce(1, false).saveAsTextFile("/home/chensha/stephanie/output_data/20160908/method3_filtercountsinglepair")
          
         
         //过滤条件4,是过滤3之后的后续步骤，并不是取代条件3：对满足过滤条件3的身份对继续分等级，
         //原因是仍然存在某些id（设为id1）与2个及以上不同的其他id（设为id2 id3）一起出现，故对这些其他id进行分级，来定义id1与id2 id3的关联性强弱。
         val judgecond4=method3_filtercountsinglepair.map(x=>{
              val id1id2=x._1._1._1.split(";")
              val numdouble=x._1._1._2.toDouble
              val numfirst=x._1._2.toDouble
              val numsecd=x._2.toDouble
              if(numdouble/numfirst>=0.6){
                if(numdouble/numfirst>=0.8){
                  if(numdouble/numsecd>=0.8){
                          (x._1._1._1,7)
                  }else{
                       if(numdouble/numsecd>=0.6){
                         (x._1._1._1,6)
                         }else{
                           if(numdouble/numsecd>=0.4){
                               (x._1._1._1,4)
                           }else{(x._1._1._1,3)}
                         }
                  }                  
                }else{
                  if(numdouble/numsecd>=0.8){
                    (x._1._1._1,6)
                  }else{
                    if(numdouble>=0.6){
                      (x._1._1._1,5)
                    }else{
                         if(numdouble/numsecd>=0.4){
                           (x._1._1._1,2)
                         }else{(x._1._1._1,1)}
                    }
                  }
                }
              }else{
                if(numdouble/numfirst>=0.4){
                  if(numdouble/numfirst>=0.8){
                      (x._1._1._1,4)
                  }else{
                    (x._1._1._1,2)
                  }
                }else{
                     if(numdouble/numsecd>=0.8){
                       (x._1._1._1,3)
                     }else{
                       (x._1._1._1,1)
                     }
                }
              }
              })
           val rankpairs=judgecond4.flatMap(x=>{
               var array=new ArrayBuffer[(String,(String,Int))]();
               val str=x._1.split(";");
               for(i<-0 until str.length){
                 array+=((str(i),x))
                    }
                  array
             })
           val rankrdd1=rankpairs.map(x=>{
                   (x._1,x._2._1+","+x._2._2.toInt)  
             }).reduceByKey(_+"::"+_).values.distinct
             //rankrdd1.collect.foreach(x=>(println(x)))
        
            val rankrdd2=rankrdd1.filter(_.split("::").length==1).distinct
            val rankrdd3=rankrdd1.filter(_.split("::").length!=1).map(x=>{
               val part=x.split("::")
               var array=new ArrayBuffer[String]()
                //对一行内的数据按照身份对的等级进行排序，用冒泡排序，如（id1;id2,3::id1;id3,2::id1:id4,5）
                        for(i<-0 until part.length){
                            for(j<-0 until part.length-1-i){
                              if(part(j).split(",")(1).toInt<part(j+1).split(",")(1).toInt){
                                    val temp=part(j)
                                    part(j)=part(j+1)
                                    part(j+1)=temp                                 
                              }else{
                                      part(j)=part(j)
                                      part(j+1)=part(j+1)
                              }
                              
                            }
                            array+=part(i)                            
                        }
               if(array.apply(0).split(",")(1).toInt==7||array.apply(0).split(",")(1).toInt==6){
                    if((array.apply(0).split(",")(1).toInt-array.apply(1).split(",")(1).toInt)<=2){
                        array.apply(0)+"::"+array.apply(1)
                    }else{
                      array.apply(0)
                        }
               }else{
                   if(array.apply(0).split(",")(1).toInt==5){
                    if((array.apply(0).split(",")(1).toInt-array.apply(1).split(",")(1).toInt)<=1){
                       array.apply(0)+"::"+array.apply(1)
                    }else{
                      array.apply(0)
                      }
                 }else{
                   if(array.apply(0).split(",")(1).toInt==4){
                    if((array.apply(0).split(",")(1).toInt-array.apply(1).split(",")(1).toInt)==0){
                      array.apply(0)+"::"+array.apply(1)
                    }else{array.apply(0)}
                   }
                   else{
                          array.apply(0)
                         }
                     }
                   }
                              
               //  array.apply(0)+"::"+array.apply(1)
             })
            // rankrdd3.coalesce(1, false).saveAsTextFile("/home/chensha/stephanie/output_data/20160908/rankrdd3")
           val rankrdd3_1=rankrdd1.filter(_.split("::").length!=1).map(x=>{
               val part=x.split("::")
               var array=new ArrayBuffer[String]()
                //对一行内的数据按照身份对的等级进行排序，用冒泡排序，如（id1;id2,3::id1;id3,2::id1:id4,5）
                        for(i<-0 until part.length){
                            for(j<-0 until part.length-1-i){
                              if(part(j).split(",")(1).toInt<part(j+1).split(",")(1).toInt){
                                    val temp=part(j)
                                    part(j)=part(j+1)
                                    part(j+1)=temp                                 
                              }else{
                                      part(j)=part(j)
                                      part(j+1)=part(j+1)
                              }
                              
                            }
                            array+=part(i)                            
                        }
                 array
             })
             
             //rankrdd3_1.coalesce(1, false).saveAsTextFile("/home/chensha/stephanie/output_data/20160908/rankrdd3_1")
             
           val rankrdd4=rankrdd3.flatMap(_.split("::")).union(rankrdd2).distinct
           //rankrdd4.coalesce(1, false).saveAsTextFile("/home/chensha/stephanie/output_data/singlerecord_process/analysjudgecond_m3")
            //将能相互合一的形式合并成一个，即有A B;B A这种形式输出为A B的形式。 
           val finalpair4_2=rankrdd4.map(x=>{x.split(",")(0)})
          finalpair4_2.coalesce(1, false).saveAsTextFile("/home/chensha/stephanie/output_data/20160908/finalpair_m4")
        //println("finalpair1 total ids: "+finalpair1.flatMap(_.split(";")).distinct.count)      
        //println("finalpair2 total ids: "+finalpair2.flatMap(_.split(";")).distinct.count)
          println("finalpair3 total ids: "+finalpair3_2.flatMap(_.split(";")).distinct.count)
          println("finalpair4 total ids: "+finalpair4_2.flatMap(_.split(";")).distinct.count)
	}
}