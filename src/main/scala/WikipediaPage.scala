import scala.util.matching.Regex

class WikipediaPage {

  protected val XML_START_TAG_TEXT: Regex = "<text xml:space=\"preserve\" bytes=\"\\d+\">".r

  protected val XML_END_TAG_TEXT = "</text>"

  protected var title: String = _

  protected var text: String = _


  class WikipediaPage(page: String) {
    def create(): Unit = println(page)
  }

}
