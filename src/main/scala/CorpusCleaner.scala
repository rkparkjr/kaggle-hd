
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame}

class CorpusCleaner extends Serializable {
  case class IndexedCorpus(word: String)

  /**
    * Cleans a string by splitting camelCase words (into 'camel case') replacing punctuation with spaces (e.g. "e g ")
    * and removing all non-letter words, turning the resulting space separated string into a list of words.
    *
    * @param s
    * @return
    */
  def cleanString(s: String): List[String] = {
    s.replaceAll("([a-z])([A-Z])", "$1 $2")
      .replaceAll("\\p{Punct}", " ")
      .replaceAll("[^a-zA-Z]", " ")
      .toLowerCase()
      .split("\\s+")
      .toList
  }

  /**
    * Uses the {@link #cleanString(String) cleanString} method to clean a list of strings containing multiple words
    * into a new RDD of cleaned strings of single words
    *
    * @param l
    * @return
    */
  def cleanListOfString(l: List[String]) : List[String] = {
    l.map(cleanString(_)).flatMap(listOfWords => listOfWords.toSeq)
  }

  /**
    * Uses the {@link #cleanString(String) cleanString} method to clean an RDD of strings containing multiple words
    * into a new RDD of cleaned strings of single words
    *
    * @param r
    * @return
    */
  def cleanRDDOfString(r: RDD[String]) : RDD[String] = {
    r.map(cleanString(_)).flatMap(listOfWords => listOfWords.toSeq).filter(w => !w.isEmpty)
  }

  /**
    * Takes a Dataframe with a column containing multi-word text (of dubious cleanliness), cleans up the text and
    * returns a new Dataframe that indexes all the words found in a single field of the input DataFrame.
    * The output Dataframe consists of with two columns: word, index
    *
    * E.g.
    *
    *  id | text
    *  1  | quick brown fox
    *  2  | jumpedOver the lazy dog
    *
    *  Result: cleanAndIndex(sqlContext, inputDf, "text", "word index")
    *
    *  text  | word index
    *  quick | 3.0
    *  brown | 2.0
    *  fox   | 6.0
    *  jumped| 0.0
    *  over  | 4.0
    *  lazy  | 5.0
    *  the   | 1.0
    *  dog   | 7.0
    *
    *  TODO: remove stopwords
    *
    * @param sqlContext - required for Dataframe conversion
    * @param df
    * @param fieldToClean
    * @param fieldForIndices
    * @return
    */
  def cleanAndIndex(sqlContext: SQLContext, df: DataFrame, fieldToClean: String, fieldForIndices: String) : DataFrame = {
    import sqlContext.implicits._
    val indexer = new StringIndexer().setInputCol("word").setOutputCol(fieldForIndices)
    val fieldCleansedDF = cleanRDDOfString(df.select(fieldToClean).map(row => row.toString)).map(word => IndexedCorpus(word)).toDF()
    indexer.fit(fieldCleansedDF).transform(fieldCleansedDF)
  }

  /**
    * TODO: Provides a new column in a Dataframe that indexes the words of another column in the data frame using a
    * Dataframe that provides an indexed list of words.
    *
    * E.g. Given the following input Dataframe
    *
    *  id | text
    *  1  | quick brown fox
    *  2  | jumpedOver the lazy dog
    *
    *  and the following indexed words Dataframe
    *
    *  text  | word index
    *  quick | 3.0
    *  brown | 2.0
    *  fox   | 6.0
    *  jumped| 0.0
    *  over  | 4.0
    *  lazy  | 5.0
    *  the   | 1.0
    *  dog   | 7.0
    *
    *  And the following call:
    *
    *  cleanAndInplaceIndex(sqlContext, inputDf, "text", "word index", "indexed words", wordIndexDF)
    *
    *  Modified the input Dataframe with an added column as below:
    *
    *  id | text                      | indexed words
    *   1 | quick brown fox           | List(3.0, 2.0, 6.0)
    *   2 | jumpedOver the lazy dog   | List(0.0, 4.0, 1.0, 5.0, 7.0)
    *
    * @param sqlContext
    * @param inputDf
    * @param fieldToClean
    * @param indexedWordsDf
    * @return
    */
  def cleanAndInplaceIndex(sqlContext: SQLContext, inputDf: DataFrame, fieldToClean: String, fieldIndex:String,
                           fieldToAdd: String, indexedWordsDf: DataFrame) : DataFrame = {
    inputDf
  }
}
