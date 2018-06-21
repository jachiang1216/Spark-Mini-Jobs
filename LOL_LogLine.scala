//Takes in a League of Legends Patch Log and formats it in 3 categories: log_id, level, message
//If the message involves describing the details, the details are omitted.
//League of Legends Patch Log Spark Job 
//Programmed by Jian An Chiang
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

//Output Format
case class LOLLog(log_id:String,level:String,message:String)

object LOL_LogLine{
  def main(args: Array[String]){
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    
    //Log File Matching
    val regex="""([\d\.]+)\| *([^\s]+)\| (.*$)""".r
    val regex2="""([\d\.]+)\| *([^\s]+)\| (The following message is
    prepared to be sent to dradis:)""".r //Disregard Message Details.
    
    val rdd=sc.textFile("/LOLPatch_Input")
    val rdd2=rdd.flatMap(x=>x match{
      case regex2(log_id,level,message)=> //Replace Message Details with the following
        new Some(LOLLog(log_id, level, "Prepared message to be sent to dradis"))
      case regex(log_id,level,message)=> //Other data
        new Some(LOLLog(log_id, level, message))
      case _ => None; //Disregard other data formats
    })
    rdd2.saveAsTextFile("LOLPatch_Output")
  }
}