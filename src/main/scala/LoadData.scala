import java.util.Properties

import com.databricks.spark.xml._
import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, SentencesAnnotation, TokensAnnotation}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import org.apache.spark.ml.feature.{CountVectorizer, IDF}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition}
import org.apache.spark.sql.functions.{col, size}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{Map, mutable}
import scala.collection.mutable.ArrayBuffer

object LoadData {

  val pipeline1: StanfordCoreNLP = createNLPPipeline()

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark Job for Loading Data").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder.getOrCreate()
    val wikiData = spark.read.option("rowTag", "page").xml("src/main/resources/small.xml")

    val dataFrame = wikiData.select("title", "revision.text._VALUE").toDF("title", "text")

    val stopWords = scala.io.Source.fromFile("src/main/resources/stopwords.txt").getLines().toSet

    val terms: DataFrame = dataFrame
      .map((t: Row) => (t.getAs("title"): String, plainTextToLemmas(t.getAs("text"), stopWords, pipeline1): String))(Encoders.tuple(Encoders.STRING, Encoders.STRING)).toDF("title", "terms")

    val trd = terms.rdd.map((row: Row) => (
      row.getAs("title"): String, row.getAs("terms").toString.split(","): Seq[String]
    ))

    val wikiDF = spark.createDataFrame(trd).toDF("title", "terms")

    //the total frequency of each term sorted DSC
    val termsCount = wikiDF.rdd.flatMap((row: Row) => row.getAs("terms"): Seq[String]).map((w: String) => (w, 1)).reduceByKey(_ + _).sortBy(f => -f._2)

    /*termsCount.foreach(f => {
      println("Term: " + f._1 + "\t\t Occurred: " + f._2)
    })

    wikiDF.foreach((row: Row) => {
      println("Title:")
      println(row.getAs("title"))
      println("Lemmas:")
      println(row.getAs("terms"))
      println("--------")
    })*/

    val filtered = wikiDF.where(size(col("terms")).>=(100))

    val countVectorizer = new CountVectorizer().setInputCol("terms").setOutputCol("termFrequency").setVocabSize(20000)
    val vocabModel = countVectorizer.fit(filtered)

    // docTermFrequency contains title; terms; termFrequency.
    // term frequency of each term with respect to each document
    val docTermFrequency = vocabModel.transform(filtered)

    docTermFrequency.show()

    docTermFrequency.cache()

    /*docTermFrequency.foreach((row: Row) => {
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
    val k = 5
    val svd = mat.computeSVD(k, computeU = true)

    val topConceptTerms = topTermsInTopConcepts(svd, 5, 6, termIds)
    val topConceptDocs = topDocsInTopConcepts(svd, 5, 15, docIds)
    for((terms, docs) <- topConceptTerms.zip(topConceptDocs)) {
      println("Concept terms: " + terms.map(_._1).mkString(", "))
      println("Concept docs: " + docs.map(_._1).mkString(", "))
      println()
    }

    val queryEngine = new LSAQueryEngine(svd, termIds, docIds, termIdfs)
    queryEngine.printTopTermsForTerm("mitochondria")
    queryEngine.printTopTermsForTerm("georgia")
    queryEngine.printTopTermsForTerm("denmark")

    queryEngine.printTopDocsForTerm("sunlight")
    queryEngine.printTopDocsForTerm("day")

    /*import spark.implicits._
    val allTerms = termIds.toSeq.toDF("term")
    val termsElasticSearch = allTerms.map(row => (row(0).asInstanceOf[String],
                                            queryEngine.getTopTermsForTerm(row(0).asInstanceOf[String]),
                                            queryEngine.getTopDocsForTerm(row(0).asInstanceOf[String])))

    termsElasticSearch.printSchema()
    termsElasticSearch.show()*/
    val result1 = termElasticSearch("sunlight", svd, termIds, docIds, termIdfs)
    println(result1._1.mkString(", "))
    println(result1._2.mkString(", "))

    val result2 = docElasticSearch("Computer", svd, termIds, docIds, termIdfs)
    println(result2._1.mkString(", "))
    println(result2._2.mkString(", "))
  }

  /*** return null if the term is not one of the interesting ones inside the vocabulary ***/
  def termElasticSearch(term: String, svd: SingularValueDecomposition[RowMatrix, Matrix], termIds: Array[String], docIds: Map[Long, String], termIdfs: Array[Double]): (Array[String], Array[String]) = {
    val queryEngine = new LSAQueryEngine(svd, termIds, docIds, termIdfs)
    if(!termIds.contains(term)){return null}
    val relTerms = queryEngine.getTopTermsForTerm(term)
    val relDocs = queryEngine.getTopDocsForTerm(term)
    return (relTerms, relDocs)
  }

  /*** return null if the doc doesn't exist in the data set ***/
  def docElasticSearch(doc: String, svd: SingularValueDecomposition[RowMatrix, Matrix], termIds: Array[String], docIds: Map[Long, String], termIdfs: Array[Double]): (Array[String], Array[String]) = {
    val queryEngine = new LSAQueryEngine(svd, termIds, docIds, termIdfs)
    if(!docIds.exists(_._2 == doc)){return null}
    val relTerms = queryEngine.getTopTermsForDoc(doc)
    val relDocs = queryEngine.getTopDocsForDoc(doc)
    return (relTerms, relDocs)
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
