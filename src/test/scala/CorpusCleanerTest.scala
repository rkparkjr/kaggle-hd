import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.scalatest._

class CleanStringTest extends FlatSpec with Matchers {

  val cc = new CorpusCleaner

  val conf = new SparkConf()
      .setAppName("Clean And Index Test")
      .setMaster("local[4]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  "Clean String" should "strip numbers" in {
    val testString = "abc123"
    cc.cleanString(testString) should be (List.fill(1)(testString.replaceAll("[0-9]", "")))
  }

  "Clean String" should "strip punct" in {
    val testString = "abc!?."
    cc.cleanString(testString) should be (List.fill(1)(testString.replaceAll("\\p{Punct}", "")))
  }

  "Clean String" should "handle parens" in {
    val testString = "abc(def)"
    cc.cleanString(testString) should be (List("abc", "def"))
  }

  "Clean String" should "split camelCase" in {
    val testString = "camelCase"
    cc.cleanString(testString) should be (List("camel", "case"))
  }

  "Clean List" should "when followed by flatten, return single list" in {
    val testList = List("abc! 2def?ghi", "xyz+mnop")
    cc.cleanListOfString(testList) should be (List("abc", "def", "ghi", "xyz", "mnop"))
  }

  "Clean RDD" should "when followed by flatten, return single list" in {
    val testRDD = sc.parallelize(List("abc! 2def?ghi", "xyz+mnop"))
    cc.cleanRDDOfString(testRDD).collect().toList should be (List("abc", "def", "ghi", "xyz", "mnop"))
  }

  "Clean and Index" should "clean RDD of words and return indexed list" in {
    import sqlContext.implicits._
    val testDF = sc.parallelize(List("abc! 2def?ghi", "xyz+mnop")).toDF()
    cc.cleanAndIndex(sqlContext, testDF, "_1", "indexed_words").count() shouldEqual(5)
  }
}
