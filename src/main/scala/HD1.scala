import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{StringIndexer, Tokenizer, StopWordsRemover}
import org.apache.spark.mllib.feature.{IDF, HashingTF}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.stat.{Statistics, MultivariateStatisticalSummary}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object HD1 {

  val productStruct = StructType(List(StructField("product_uid", LongType, nullable = false),
    StructField("product_description", StringType, nullable = false)
  ))
  val productAttributeStruct = StructType(List(StructField("product_uid", LongType, nullable = false),
    StructField("name", StringType, nullable = false),
    StructField("value", StringType, nullable = false)
  ))

  val trainStruct = StructType(List(StructField("id", LongType, nullable = false),
    StructField("product_uid", LongType, nullable = false),
    StructField("product_title", StringType, nullable = false),
    StructField("search_term", StringType, nullable = false),
    StructField("relevance", DoubleType, nullable = false)
  ))
  val testStruct = StructType(List(StructField("id", LongType, nullable = false),
    StructField("product_uid", LongType, nullable = false),
    StructField("product_title", StringType, nullable = false),
    StructField("search_term", StringType, nullable = false)
  ))

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Home Depot Analysis 1")
      .setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    var showRelevanceStats = false
    var showTopSearchTeams = false
    var showHighlyRelevant = false
    var generateTFIDFs = false
    var generateStringIdx = false
    var showTopTermsAll = true

    val dataPath = "/Users/ddcronaldp/homedepot/data/"

    val productDf = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(dataPath + "product_descriptions.csv")

    val productAttributesDf = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(dataPath + "attributes.csv")

    val trainDf = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(dataPath + "train.csv")

    val testDf = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(dataPath + "train.csv")

    if (showRelevanceStats) {
      examineRelevanceStats(trainDf)
    }

    if (showTopSearchTeams) {
      val searchTermWordsCounted = trainDf
        .flatMap(t => t.getAs[String]("search_term").split(" "))
        .filter(word => word.length > 0)
        .map(word => (word, 1))
        .reduceByKey((a, b) => a + b)

      val searchTermTopWords = searchTermWordsCounted.map(word_count => (word_count._2, word_count._1))

      val maxTerm = searchTermTopWords.max();
      System.out.println(maxTerm)
    }
    val stopWords = sc.broadcast(
      Set("a", "about", "above", "according", "accordingly", "across", "actually", "..."))

    if (showTopTermsAll) {
      val searchTermWords = trainDf
        .flatMap(t => t.getAs[String]("search_term").split(" "))
        .filter(word => word.length > 0)
      val productDescriptionWords = productDf
        .flatMap(t => t.getAs[String]("product_description").split(" "))
        .filter(word => word.length > 0)
      val productAttributeDescriptionWords = productAttributesDf
        .flatMap(t => t.getAs[String]("value").split(" "))
        .filter(word => word.length > 0)
      val allWords = searchTermWords.union(productDescriptionWords).union(productAttributeDescriptionWords)
        .map(cleanString(_))
          .flatMap(listOfWords => listOfWords.toSeq)
        .map(word => (word, 1))
        .reduceByKey((a, b) => a + b)
      val topWords = allWords.map(word_count => (word_count._2, word_count._1)).toDF()
      val indexer = new StringIndexer().setInputCol("_2").setOutputCol("indexedWords")
      val indexedDf = indexer.fit(topWords).transform(topWords)
      indexedDf.toJavaRDD.saveAsTextFile(dataPath + "topwords-indexed")
    }

    if (showHighlyRelevant) {
      val highlyRelevant = trainDf.join(productDf, "product_uid").select("product_uid", "product_title", "search_term", "relevance").where("relevance > 2.0")

      val highlyRelevantProducts = highlyRelevant.groupBy("product_uid")

      highlyRelevantProducts.count().sort(desc("count"), asc("product_uid")).show()
    }

    if (generateTFIDFs) {
      generateTermFreqIDF(dataPath, trainDf, "train", "search_term")
      generateTermFreqIDF(dataPath, productDf, "product", "product_description")
    }

    if (generateStringIdx) {
      generateStringIndexer(dataPath, trainDf, "train", "search_term")
    }
  }

  def cleanString(s: String): List[String] = {
    s.replaceAll("([a-z])([A-Z])", "$1 $2")
      .replaceAll("\\p{Punct}", " ")
      .replaceAll("[^a-zA-Z]", " ")
      .toLowerCase()
      .split("\\s+")
      .toList
  }

  def generateStringIndexer(dataPath: String, df: DataFrame, dfName: String, field: String): Unit = {
    val tokenizer = new Tokenizer().setInputCol(field).setOutputCol("words")
    val indexer = new StringIndexer().setInputCol(field).setOutputCol("indexedWords")
    val wordsDf = tokenizer.transform(df)
    val seqStopped = wordsDf.map(_.getAs[Seq[String]]("words").toIterable)
    val indexedDf = indexer.fit(df).transform(df)
    df.select(field).na.drop()
    indexedDf.show()
  }

  def generateTermFreqIDF(dataPath: String, df: DataFrame, dfName: String, field: String): Unit = {
    val tokenizer = new Tokenizer().setInputCol(field).setOutputCol("words")
    val stopWordsRemover = new StopWordsRemover().setInputCol("words").setOutputCol("stopped")
    val wordsDf = tokenizer.transform(df)
    val stoppedDf = stopWordsRemover.transform(wordsDf)
    val seqStopped = stoppedDf.map(_.getAs[Seq[String]]("stopped").toIterable)
    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(seqStopped)
    tf.cache()
    val idf = new IDF(minDocFreq = 2).fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)
    tf.saveAsTextFile(dataPath + dfName + "-" + field + "-tf")
    tfidf.saveAsTextFile(dataPath + dfName + "-" + field + "-tfidf")
  }

  def examineRelevanceStats(trainDf: DataFrame): Unit = {
    val relevanceVector = trainDf.map { r =>
      val array = Array(r.getAs[Double]("relevance"))
      Vectors.dense(array)
    }

    val summary: MultivariateStatisticalSummary = Statistics.colStats(relevanceVector)
    println(summary.count)
    println(summary.mean)
    println(summary.variance)
    println(summary.numNonzeros)
    println(summary.max)
    println(summary.min)
  }
}
