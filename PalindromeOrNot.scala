//Labels words in textfile if they are palindromes or not and counts them
//Simple Spark Job 
//Programmed by Jian An Chiang
//Import Spark Configurations
import org.apache.spark.SparkContext 
import org.apache.spark.SparkConf 

object WordCount {
   //----------------------Main Method-----------------------------------------------
   def main(args: Array[String]){
     val conf = new SparkConf().setAppName("Simple Application")
     val sc = new SparkContext(conf)
     //When running this, make sure that the file is in HDFS
     val input = sc.textFile("/PDrome_Input")
     val words = input.flatMap(line=>line.split(" "))
     val counts = words.map(word=>(palindrome(word),1)).reduceByKey(_+_)
     //Save it to a text file in HDFS
     counts.saveAsTextFile("/PDrome_Output")
   }
   //----------------------Palindrome Detection Method-------------------------------- 
   def palindrome(x:String):String={
     if (x!=x.reverse){
       return "Palindrome"
     }else{
       return "Not Palindrome"
     }
   }
}