import java.util
import java.util.UUID

import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}

class ElasticClient() {

  final val host = "0.0.0.0"
  final val port = 9200
  final val scheme = "http"

  final val client = createClient()

  private def createClient(): RestHighLevelClient = {
    val client = new RestHighLevelClient(
      RestClient.builder(
        new HttpHost(host, port, scheme)
      ))
    client
  }

  def putToIndex(index: String, data: util.HashMap[String, AnyRef]): String = {
    val request = new IndexRequest(index: String).id(UUID.randomUUID.toString).source(data)
    val indexResponse = client.index(request, RequestOptions.DEFAULT)
    indexResponse.getResult.name()
  }
}
