import java.io.FileWriter
import org.apache.spark._
import scala.util.parsing.json.JSON

object zhengyi_wang_task2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("task2").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val review_file = sc.textFile(args(0))
    val business_file = sc.textFile(args(1))

    // A. The average stars for each state
    val review_rdd = review_file.map(x => JSON.parseFull(x))
      .map(x => x.get.asInstanceOf[Map[String, Any]])
      .map(x => (x("business_id").asInstanceOf[String], x("stars").asInstanceOf[Double])).persist()

    val business_rdd = business_file.map(x => JSON.parseFull(x))
      .map(x => x.get.asInstanceOf[Map[String, Any]])
      .map(x => (x("business_id").asInstanceOf[String], x("state").asInstanceOf[String])).persist()

    val business_review = business_rdd.join(review_rdd).map(x => (x._2._1, (x._2._2, 1)))
    val state_avg = business_review.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map(x => (x._1, x._2._1 / x._2._2))
      .sortByKey(true).map(x=> (x._2,x._1))
      .sortByKey(false).map(x=> (x._2,x._1))

    val result = state_avg.collect().mkString("\n").replaceAll("\\(","").replaceAll("\\)","")
    val writer = new FileWriter(args(2))
    writer.write("state,stars \n")
    writer.write(result)
    writer.close()

    //B. Two method of print Top 5 states
    val start_1=System.currentTimeMillis()
    val result_1=state_avg.collect()
    for (i <- 0 to 4){
      println(result_1(i))
    }
    val end_1=System.currentTimeMillis()
    val Method_1_time=end_1-start_1

    val start_2=System.currentTimeMillis()
    val result_2=state_avg.take(5).foreach(println)
    val end_2=System.currentTimeMillis()
    val Method_2_time=end_2-start_2

    val writer1 = new FileWriter(args(3))
    writer1.write("{\n")
    writer1.write("\"m1\":"+Method_1_time+",\n")
    writer1.write("\"m2\":"+Method_2_time+",\n")
    writer1.write("\"explanation\":"+ "\"For method 1, you need to transfer all the data from the RDD to list, but for method 2, the operation take(5) let the computer only need transfer the top 5 data from the RDD to list. Because the second method need to handle less data than the first method, the Method 2 spends less time than Method 1.\""+"\n")
    writer1.write("}")
    writer1.close()
  }

}
