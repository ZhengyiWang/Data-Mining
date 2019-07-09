
import java.io.FileWriter
import org.apache.spark._
import scala.util.parsing.json.JSON


object zhengyi_wang_task1 {
  def main(args: Array[String]): Unit={
    // Create spark configuration
    val conf = new SparkConf().setAppName("task1").setMaster("local[*]")
    val sc=new SparkContext(conf)

    val input_file=sc.textFile(args(0))

    val rdd = input_file.map(x=>JSON.parseFull(x))
      .map(x=>x.get.asInstanceOf[Map[String, Any]])
      .map(x=>(x("useful").asInstanceOf[Double], x("stars").asInstanceOf[Double],
        x("text").asInstanceOf[String], x("user_id").asInstanceOf[String], x("business_id").asInstanceOf[String])).persist()

    // A. The number of reviews that people think are useful (The value of tag ‘useful’ > 0)
     val useful_count=rdd.filter(x=> x._1>0).count()

    //B. The number of reviews that have 5.0 stars rating
     val rating_count=rdd.filter(x=> x._2==5).count()

    //C. How many characters are there in the ‘text’ of the longest review
     val longest_characters=rdd.map(x=> x._3.length).max()

    //D. The number of distinct users who wrote reviews
     val user_list=rdd.map(x=>x._4).persist()
     val user_count=user_list.distinct().count()

    //E. The top 20 users who wrote the largest numbers of reviews and the number of reviews they wrote
     val user_top=user_list.map(x=> (x,1)).reduceByKey(_+_)
       .sortByKey(true).map(x=> (x._2,x._1))
       .sortByKey(false).map(x=> (x._2,x._1)).take(20).mkString(",")

    val user_json=user_top.replaceAll("\\(","[\"").replaceAll(",","\"\\,").replaceAll("\\)","]")
      .replaceAll("\"\\,\\[",",[")

    //F. The number of distinct businesses that have been reviewed
     val business_list=rdd.map(x=>x._5).persist()
     val business_count=business_list.distinct().count()

    //G. The top 20 businesses that had the largest numbers of reviews and the number of reviews they had
     val business_top=business_list.map(x=> (x,1)).reduceByKey(_+_)
       .sortByKey(true).map(x=> (x._2,x._1))
       .sortByKey(false).map(x=> (x._2,x._1)).take(20).mkString(",")

    val business_json=business_top.replaceAll("\\(","[\"").replaceAll(",","\"\\,").replaceAll("\\)","]")
      .replaceAll("\"\\,\\[",",[")

    val writer = new FileWriter(args(1))
    writer.write("{\n")
    writer.write("\"useful_count\":"+useful_count+",\n")
    writer.write("\"5_stars_rating\":"+rating_count+",\n")
    writer.write("\"longest_characters\":"+longest_characters+",\n")
    writer.write("\"distinct_users\":"+user_count+",\n")
    writer.write("\"top_20_users\":"+"["+user_json+"],\n")
    writer.write("\"distinct_business\":"+business_count+",\n")
    writer.write("\"top20_business\":"+"["+business_json+"]\n")
    writer.write("}")

    writer.close()
  }
}
