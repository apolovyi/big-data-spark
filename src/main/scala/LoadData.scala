
//import java.util.Properties

import java.util.Properties

import scala.collection.JavaConverters._
import com.databricks.spark.xml._
import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, SentencesAnnotation, TokensAnnotation}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import org.apache.spark.ml.feature.{CountVectorizer, IDF}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition}
import org.apache.spark.sql.functions.{col, size}
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

object LoadData {

  val pipeline1: StanfordCoreNLP = createNLPPipeline()

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark Job for Loading Data").setMaster("local[*]") // local[*] will access all core of your machine
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder.getOrCreate()
    val wikiData = spark.read.option("rowTag", "page").xml("src/main/resources/sm.xml")


    val dataFrame = wikiData.select("title", "revision.text._VALUE")

    val props: Properties = new Properties()
    props.put("annotators", "tokenize, ssplit, pos, lemma")


    val stopWords = scala.io.Source.fromFile("src/main/resources/stopwords.txt").getLines().toSet

    val terms: DataFrame = dataFrame
      .map((t: Row) => (t.getAs("title"): String, plainTextToLemmas(t.getAs("_VALUE"), stopWords, pipeline1): String))(Encoders.tuple(Encoders.STRING, Encoders.STRING)).toDF("title", "terms")

    val trd = terms.rdd.map((row: Row) => (
      row.getAs("title"): String, row.getAs("terms").toString.split(","): Seq[String]
    ))

    val wikiDF = spark.createDataFrame(trd).toDF("title", "terms")

    wikiDF.foreach((row: Row) => {
      println("Title:")
      println(row.getAs("title"))
      println("Lemmas:")
      println(row.getAs("terms"))
      println("--------")
    })

    val filtered = wikiDF.where(size(col("terms")).>=(100))

    val countVectorizer = new CountVectorizer().setInputCol("terms").setOutputCol("termFrequency").setVocabSize(200)
    val vocabModel = countVectorizer.fit(filtered)

    // docTermFrequency contains title; terms; termFrequency.
    // term frequency of each term with respect to each document
    val docTermFrequency = vocabModel.transform(filtered)

    docTermFrequency.cache()

    /*docTermFrequency.foreach((row: Row) => {
      println("Title: " + row.getAs("title"))
      println("Terms: " + row.getAs("terms"))
      println("TermFrequency: " + row.getAs("termFrequency"))
      println()
    })*/

    val idf = new IDF().setInputCol("termFrequency").setOutputCol("tfidfVec")
    val idfModel = idf.fit(docTermFrequency)
    val docTermMatrix = idfModel.transform(docTermFrequency).select("title", "tfidfVec")

    /*docTermMatrix.foreach((row: Row) => {
      println()
      println("Title: " + row.getAs("title"))
      println("tfidfVec: " + row.getAs("tfidfVec"))
      println()
    })*/

    val termIds: Array[String] = vocabModel.vocabulary

    val docIds = docTermFrequency.rdd.map(_.getString(0)).
      zipWithUniqueId().
      map(_.swap).
      collect().toMap


    val vecRdd = docTermMatrix.select("tfidfVec").rdd.map {
      row => org.apache.spark.mllib.linalg.Vectors.fromML(row.getAs[MLVector]("tfidfVec"))
    }

    vecRdd.cache()

    val mat = new RowMatrix(vecRdd)
    val k = 20
    val svd = mat.computeSVD(k, computeU = true)

    val topConceptTerms = topTermsInTopConcepts(svd, 10, 15, termIds)
    val topConceptDocs = topDocsInTopConcepts(svd, 10, 5, docIds)
    for((terms, docs) <- topConceptTerms.zip(topConceptDocs)) {
      println("Concept terms: " + terms.map(_._1).mkString(", "))
      println("Concept docs: " + docs.map(_._1).mkString(", "))
      println()
    }

    print(trd)
  }

  def createNLPPipeline(): StanfordCoreNLP = {
    val props = new Properties()
    props.put("annotators", "tokenize, ssplit, pos, lemma")
    new StanfordCoreNLP(props)
  }

  def isOnlyLetters(str: String): Boolean = {
    str.forall(c => Character.isLetter(c))
  }

  def plainTextToLemmas(text: String, stopWords: Set[String], pipeline: StanfordCoreNLP): String = {
    val doc = new Annotation(text)
    pipeline.annotate(doc)
    //val lemmas = new ArrayBuffer[String]()
    var lemmas: String = ""
    val sentences = doc.get(classOf[SentencesAnnotation]).asScala
    for(sentence <- sentences; token <- sentence.get(classOf[TokensAnnotation]).asScala) {
      val lemma: String = token.get(classOf[LemmaAnnotation])
      if(lemma.length > 2 && !stopWords.contains(lemma) && isOnlyLetters(lemma)) {
        //lemmas= lemma.toLowerCase
        lemmas = lemmas.concat(lemma).concat(",")
      }
    }
    lemmas
  }

  def topTermsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix], numConcepts: Int,
                            numTerms: Int, termIds: Array[String]): Seq[Seq[(String, Double)]] = {
    val v = svd.V
    val topTerms = new ArrayBuffer[Seq[(String, Double)]]()
    val arr = v.toArray
    for(i <- 0 until numConcepts) {
      val offs = i * v.numRows
      val termWeights = arr.slice(offs, offs + v.numRows).zipWithIndex
      val sorted = termWeights.sortBy(-_._1)
      topTerms += sorted.take(numTerms).map { case (score, id) => (termIds(id), score) }
    }
    topTerms
  }

  def topDocsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix], numConcepts: Int,
                           numDocs: Int, docIds: Map[Long, String]): Seq[Seq[(String, Double)]] = {
    val u = svd.U
    val topDocs = new ArrayBuffer[Seq[(String, Double)]]()
    for(i <- 0 until numConcepts) {
      val docWeights = u.rows.map(_.toArray(i)).zipWithUniqueId
      topDocs += docWeights.top(numDocs).map { case (score, id) => (docIds(id), score) }
    }
    topDocs
  }

}
