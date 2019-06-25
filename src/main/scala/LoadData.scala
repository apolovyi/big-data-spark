
//import java.util.Properties

import java.util.Properties

import com.databricks.spark.xml._
import edu.stanford.nlp.ling.CoreAnnotations.{SentencesAnnotation, TextAnnotation, TokensAnnotation}
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.util.CoreMap
import org.apache.spark.sql.DataFrame
//import org.apache.spark
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._


//import scala.collection.mutable.ArrayBuffer
//import com.databricks.spark.corenlp.functions._
//import edu.stanford.nlp.pipeline._
//import edu.stanford.nlp.ling.CoreAnnotations._

object LoadData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark Job for Loading Data").setMaster("local[*]") // local[*] will access all core of your machine
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder.getOrCreate()
    val df = spark.read.option("rowTag", "page").xml("src/main/resources/train.xml")

    val dataFrame = df.select("title", "revision.text._VALUE")
    //println(dataFrame.first())

    //val title = dataFrame.select("title")
    //val text = dataFrame.select("text._VALUE")
    //val text2 = dataFrame.select("text._bytes")
    //val text3 = dataFrame.select("text._space")

    println(dataFrame.first())


    val props: Properties = new Properties()
    props.put("annotators", "tokenize, ssplit, pos, lemma")

    val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)

    // read some text from a file - Uncomment this and comment the val text = "Quick...." below to load from a file
    //val inputFile: File = new File("src/test/resources/sample-content.txt")
    //val text: String = Files.toString(inputFile, Charset.forName("UTF-8"))
    val text3 = "Quick brown fox jumps over the lazy dog. This is Harshal."

    // create blank annotator
    val document: Annotation = new Annotation(text3)

    // run all Annotator - Tokenizer on this text
    pipeline.annotate(document)

    val sentences: List[CoreMap] = document.get(classOf[SentencesAnnotation]).asScala.toList

    (for {
      sentence: CoreMap <- sentences
      token: CoreLabel <- sentence.get(classOf[TokensAnnotation]).asScala.toList
      word: String = token.get(classOf[TextAnnotation])

    } yield (word, token)) foreach (t => println("word: " + t._1 + " token: " + t._2))



    val rdd = sc.newAPIHadoopFile("src/main/resources/train.xml")
    print(rdd.name)

    //dataFrame.write
    //    //  .option("rootTag", "books")
    //    //  .option("rowTag", "book")
    //    //  .xml("newbooks.xml")

    // It will return a RDD
    // Read the records
    //println(emp_data.foreach(println))
  }

  //def createNLPPipeline(): StanfordCoreNLP = {
  //  val props = new Properties()
  //  props.put("annotators", "tokenize, ssplit, pos, lemma")
  //  new StanfordCoreNLP(props)
  //}
  //def isOnlyLetters(str: String): Boolean = {
  //  str.forall(c => Character.isLetter(c))
  //}
  //def plainTextToLemmas(text: String, stopWords: Set[String],
  //                      pipeline: StanfordCoreNLP): Seq[String] = {
  //  val doc = new Annotation(text)
  //  pipeline.annotate(doc)
  //  val lemmas = new ArrayBuffer[String]()
  //  val sentences = doc.get(classOf[SentencesAnnotation])
  //  for (sentence <- sentences.asScala;
  //       token <- sentence.get(classOf[TokensAnnotation]).asScala) {
  //    val lemma = token.get(classOf[LemmaAnnotation])
  //    if (lemma.length > 2 && !stopWords.contains(lemma)
  //      && isOnlyLetters(lemma)) {
  //      lemmas += lemma.toLowerCase
  //    }
  //  }
  //  lemmas
  //}
  //val stopWords = scala.io.Source.fromFile("stopwords.txt").getLines().toSet
  //val bStopWords = spark.sparkContext.broadcast(stopWords)
  //val terms: Dataset[(String, Seq[String])] =
  //  docTexts.mapPartitions { iter =>
  //    val pipeline = createNLPPipeline()
  //    iter.map { case(title, contents) =>
  //      (title, plainTextToLemmas(contents, bStopWords.value, pipeline))
  //    }
  //  }
  //
  
  
  //def toPlainText(pageXml: String): Option[(String, String)] = {
  //  // Wikipedia has updated their dumps slightly since Cloud9 was written,
  //  // so this hacky replacement is sometimes required to get parsing to work.
  //  val hackedPageXml = pageXml.replaceFirst(
  //    "<text xml:space=\"preserve\" bytes=\"\\d+\">",
  //    "<text xml:space=\"preserve\">")
  //  val page = new EnglishWikipediaPage()
  //  WikipediaPage.readPage(page, hackedPageXml)
  //  if (page.isEmpty) None
  //  else Some((page.getTitle, page.getContent))
  //}

  //def readWikiDump(sc: SparkContext, file: String) : RDD[(Long, String)] = {
  //  val conf = new Configuration()
  //  conf.set(XmlInputFormat.START_TAG_KEY, "<page>")
  //  conf.set(XmlInputFormat.END_TAG_KEY, "</page>")
  //  val rdd = sc.newAPIHadoopFile(file,
  //    classOf[XmlInputFormat],
  //    classOf[LongWritable],
  //    classOf[Text],
  //    conf)
  //  rdd.map{case (k,v) => (k.get(), new String(v.copyBytes()))}
  //}

  //def readWikiDump(sc: SparkContext, file: String): RDD[(Long, String)] = {
  //  val conf = new Configuration()
  //  sc.read
  //  conf.set(XmlInputFormat.START_TAG_KEY, "<page>")
  //  conf.set(XmlInputFormat.END_TAG_KEY, "</page>")
  //  val rdd = sc.newAPIHadoopFile(file,
  //    classOf[XmlInputFormat],
  //    classOf[LongWritable],
  //    classOf[Text],
  //    conf)
  //  rdd.map { case (k, v) => (k.get(), new String(v.copyBytes())) }
  //}
}
