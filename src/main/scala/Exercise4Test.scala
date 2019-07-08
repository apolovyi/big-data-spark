import java.util.Properties

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

import org.elasticsearch.spark.sql._

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

object Exercise4Test {

  val pipeline1: StanfordCoreNLP = createNLPPipeline()

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Spark Job for Loading Data")
      .setMaster("local[*]")
      .set("es.nodes.wan.only", "true")
      .set("es.index.auto.create", "true")

    val sc = new SparkContext(conf)

    val spark = SparkSession.builder.getOrCreate()
    val wikiData = spark.read.option("rowTag", "page").xml("src/main/resources/smallWiki.xml")

    val dataFrame = wikiData.select("title", "revision.text._VALUE", "revision.contributor.username", "revision.timestamp").toDF("title", "text", "contributor", "timestamp")

    val stopWords = scala.io.Source.fromFile("src/main/resources/stopwords.txt").getLines().toSet

    val terms: DataFrame = dataFrame
      .map((t: Row) => (t.getAs("title"): String, plainTextToLemmas(t.getAs("text"), stopWords, pipeline1): String))(Encoders.tuple(Encoders.STRING, Encoders.STRING)).toDF("title", "terms")

    val trd = terms.rdd.map((row: Row) => (
      row.getAs("title"): String, row.getAs("terms").toString.split(","): Seq[String]
    ))

    val wikiDF = spark.createDataFrame(trd).toDF("title", "terms")

    //the total frequency of each term sorted DSC
    val termsCount = wikiDF.rdd.flatMap((row: Row) => row.getAs("terms"): Seq[String]).map((w: String) => (w, 1)).reduceByKey(_ + _).sortBy(f => -f._2)

    termsCount.foreach(f => {
      println("Term: " + f._1 + "\t\t Occurred: " + f._2)
    })

    val filtered = wikiDF.where(size(col("terms")).>=(100))
    val countVectorizer = new CountVectorizer().setInputCol("terms").setOutputCol("termFrequency").setVocabSize(20000)
    val vocabModel = countVectorizer.fit(filtered)

    // docTermFrequency contains title; terms; termFrequency.
    // term frequency of each term with respect to each document
    val docTermFrequency = vocabModel.transform(filtered)

    //docTermFrequency.show()

    docTermFrequency.cache()

    /*    docTermFrequency.foreach((row: Row) => {
          println("Title: " + row.getAs("title"))
          println("Terms: " + row.getAs("terms"))
          println("TermFrequency: " + row.getAs("termFrequency"))
          println()
        })*/

    val idf = new IDF().setInputCol("termFrequency").setOutputCol("tfIdf")
    val idfModel = idf.fit(docTermFrequency)
    val docTermMatrix = idfModel.transform(docTermFrequency).select("title", "tfIdf")

    val termIdfs = idfModel.idf.toArray

    /*docTermMatrix.foreach((row: Row) => {
      println()
      println("Title: " + row.getAs("title"))
      println("TF-IDF: " + row.getAs("tfIdf"))
      println()
    })*/

    val termIds: Array[String] = vocabModel.vocabulary

    val docIds = docTermFrequency.rdd.map(_.getString(0)).zipWithUniqueId().map(_.swap).collect().toMap

    val vecRdd = docTermMatrix.select("tfIdf").rdd.map {
      row => org.apache.spark.mllib.linalg.Vectors.fromML(row.getAs[MLVector]("tfIdf"))
    }

    vecRdd.cache()

    val mat = new RowMatrix(vecRdd)
    val k = 10
    val svd = mat.computeSVD(k, computeU = true)

    val topConceptTerms = topTermsInTopConcepts(svd, 10, 6, termIds)
    val topConceptDocs = topDocsInTopConcepts(svd, 10, 15, docIds)
    for((terms, docs) <- topConceptTerms.zip(topConceptDocs)) {
      println("Concept ")
      println("Terms: " + terms.map(_._1).mkString(", "))
      println("Docs: " + docs.map(_._1).mkString(", "))
      println()
    }

    val queryEngine = new QueryEngine(svd, termIds, docIds, termIdfs)

    queryEngine.printTopDocsForDoc("Computer")
    queryEngine.printTopDocsForDoc("Ebola virus")
    queryEngine.printTopDocsForDoc("Continent")
    queryEngine.printTopDocsForDoc("City")
    queryEngine.printTopDocsForDoc("Earth")
    queryEngine.printTopDocsForDoc("Europe")

    queryEngine.printTopTermsForTerm("mitochondria")
    queryEngine.printTopTermsForTerm("georgia")
    queryEngine.printTopTermsForTerm("denmark")
    queryEngine.printTopTermsForTerm("protein")
    queryEngine.printTopTermsForTerm("serbia")
    queryEngine.printTopTermsForTerm("monarchy")
    queryEngine.printTopTermsForTerm("meal")


    spark.createDataFrame(sc.parallelize(queryEngine.prepareDocIndexMoreInfo(dataFrame))).toDF("document", "contributor", "timestamp", "topTermsForDocument", "topDocsForDocument").saveToEs("wiki-documents")
    //spark.createDataFrame(sc.parallelize(queryEngine.prepareDocIndex())).toDF("document", "topTermsForDocument", "topDocsForDocument").saveToEs("wiki-documents-basic-v2.0")
    spark.createDataFrame(sc.parallelize(queryEngine.prepareTermIndex())).toDF("term", "topTermsForTerm", "topDocsForTerm").saveToEs("wiki-terms")

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
    var lemmas: String = ""
    import scala.collection.JavaConverters._

    if(text != null) {
      val doc = new Annotation(text)
      pipeline.annotate(doc)

      val sentences = doc.get(classOf[SentencesAnnotation]).asScala

      for(sentence <- sentences; token <- sentence.get(classOf[TokensAnnotation]).asScala) {
        val lemma: String = token.get(classOf[LemmaAnnotation])
        if(lemma.length > 2 && !stopWords.contains(lemma) && isOnlyLetters(lemma)) {
          lemmas = lemmas.concat(lemma.toLowerCase).concat(",")
        }
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
