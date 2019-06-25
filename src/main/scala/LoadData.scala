
import com.databricks.spark.xml._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql.functions._
//import com.databricks.spark.corenlp.functions._

object LoadData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark Job for Loading Data").setMaster("local[*]") // local[*] will access all core of your machine
    val sc = new SparkContext(conf) // Create Spark Context
    // Load local file data
    //val emp_data = sc.textFile("src/main/resources/train.xml")
    //val test = sc.read.format("com.databricks.spark.xml").option()


    val spark = SparkSession.builder.getOrCreate()
    val df = spark.read.option("rowTag", "page").xml("src/main/resources/train.xml")

    val dataFrame = df.select("title", "revision.text._VALUE")
    //println(dataFrame.first())

    val title = dataFrame.select("title")
    val text = dataFrame.select("text._VALUE")
    //val text2 = dataFrame.select("text._bytes")
    //val text3 = dataFrame.select("text._space")

    println(title.first())
    println(text.first())
    //println(text2.first())
    //
    //println(text3.first())
    //
    //println(text3.rdd)

    //val output = dataFrame.select(cleanxml('text).as('doc)).select(explode(ssplit('doc)).as('sen)).select('sen, tokenize('sen).as('words))
    ////, ner('sen).as('nerTags), sentiment('sen).as('sentiment)
    //output.show()

    //val output1 = output.select('sen, lemma('sen).as('lemmatized))
    ////val dataFrame = df.withColumn("title", )
    ////println(dataFrame.first())
    //
    //val selectedData2 = df.select("title", "revision.text")
    //val sh = selectedData2.schema
    ////println(selectedData2.col("text"))
    //println(selectedData2.first())

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
