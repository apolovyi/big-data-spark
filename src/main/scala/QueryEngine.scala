import java.sql.Struct

import breeze.linalg.{DenseMatrix => BDenseMatrix}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrices, Matrix, SingularValueDecomposition, Vectors, Vector => MLLibVector}
import org.apache.spark.sql.{DataFrame}

import scala.collection.Map

class QueryEngine (
                   val svd: SingularValueDecomposition[RowMatrix, Matrix],
                   val termIds: Array[String],
                   val docIds: Map[Long, String],
                   val termIdfs: Array[Double]) extends java.io.Serializable {

  val VS: BDenseMatrix[Double] = multiplyByDiagonalMatrix(svd.V, svd.s)
  val normalizedVS: BDenseMatrix[Double] = rowsNormalized(VS)
  val US: RowMatrix = multiplyByDiagonalRowMatrix(svd.U, svd.s)
  val normalizedUS: RowMatrix = distributedRowsNormalized(US)

  val idTerms: Map[String, Int] = termIds.zipWithIndex.toMap
  val idDocs: Map[String, Long] = docIds.map(_.swap)

  def multiplyByDiagonalMatrix(mat: Matrix, diag: MLLibVector): BDenseMatrix[Double] = {
    val sArr = diag.toArray
    new BDenseMatrix[Double](mat.numRows, mat.numCols, mat.toArray)
      .mapPairs { case ((r, c), v) => v * sArr(c) }
  }

  def multiplyByDiagonalRowMatrix(mat: RowMatrix, diag: MLLibVector): RowMatrix = {
    val sArr = diag.toArray
    new RowMatrix(mat.rows.map { vec =>
      val vecArr = vec.toArray
      val newArr = (0 until vec.size).toArray.map(i => vecArr(i) * sArr(i))
      Vectors.dense(newArr)
    })
  }

  def rowsNormalized(mat: BDenseMatrix[Double]): BDenseMatrix[Double] = {
    val newMat = new BDenseMatrix[Double](mat.rows, mat.cols)
    for(r <- 0 until mat.rows) {
      val length = math.sqrt((0 until mat.cols).map(c => mat(r, c) * mat(r, c)).sum)
      (0 until mat.cols).foreach(c => newMat.update(r, c, mat(r, c) / length))
    }
    newMat
  }

  def distributedRowsNormalized(mat: RowMatrix): RowMatrix = {
    new RowMatrix(mat.rows.map { vec =>
      val array = vec.toArray
      val length = math.sqrt(array.map(x => x * x).sum)
      Vectors.dense(array.map(_ / length))
    })
  }

  def topDocsForTerm(termId: Int): Seq[(Double, Long)] = {
    val rowArr = (0 until svd.V.numCols).map(i => svd.V(termId, i)).toArray
    val rowVec = Matrices.dense(rowArr.length, 1, rowArr)

    val docScores = US.multiply(rowVec)

    val allDocWeights = docScores.rows.map(_.toArray(0)).zipWithUniqueId
    allDocWeights.top(50)
  }

  def topTermsForTerm(termId: Int): Seq[(Double, Int)] = {
    val rowVec = normalizedVS(termId, ::).t

    val termScores = (normalizedVS * rowVec).toArray.zipWithIndex

    termScores.sortBy(-_._1).take(10)
  }

  def topTermsForDoc(docId: Long): Seq[(Double, Int)] = {
    val docRowArr = normalizedUS.rows.zipWithUniqueId.map(_.swap)
      .lookup(docId).head.toArray
    val docRowMat = BDenseMatrix.zeros[Double](docRowArr.length, 1)
    for(i <- Range(0, docRowArr.length - 1))
      docRowMat(i, 0) = docRowArr(i)
    val docRowVec = docRowMat(::, 0)
    // Compute scores against every term
    val termScores = (VS * docRowVec).toArray.zipWithIndex

    termScores.sortBy(-_._1).take(50)
  }

  def topDocsForDoc(docId: Long): Seq[(Double, Long)] = {
    val docRowArr = normalizedUS.rows.zipWithUniqueId.map(_.swap).lookup(docId).head.toArray
    val docRowVec = Matrices.dense(docRowArr.length, 1, docRowArr)

    val docScores = normalizedUS.multiply(docRowVec)

    val allDocWeights = docScores.rows.map(_.toArray(0)).zipWithUniqueId

    allDocWeights.filter(!_._1.isNaN).top(20)
  }

  def printTopTermsForTerm(term: String): Unit = {
    val idWeights = topTermsForTerm(idTerms(term))
    println("Top terms for term: " + term)
    println(idWeights.map(f => termIds(f._2)).mkString(", "))
  }

  def printTopDocsForTerm(term: String): Unit = {
    val idWeights = topDocsForTerm(idTerms(term))
    println("Top documents for term: " + term)
    println(idWeights.map(f => docIds(f._2)).mkString(", "))
  }

  def printTopDocsForDoc(doc: String): Unit = {
    val idWeights = topDocsForDoc(idDocs(doc))
    println("Top documents for document: " + doc)
    println(idWeights.map(f => docIds(f._2)).mkString(", "))
  }

  def getTopTermsForTerm(term: String): Array[String] = {
    val idWeights = topTermsForTerm(idTerms(term))
    idWeights.map(f => {
      termIds(f._2)
    }).toArray
  }

  def getTopDocsForTerm(term: String): Array[String] = {
    val idWeights = topDocsForTerm(idTerms(term))
    idWeights.map(f => {
      docIds(f._2)
    }).toArray
  }

  def getTopTermsForDoc(doc: String): Array[String] = {
    val idWeights = topTermsForDoc(idDocs(doc))
    idWeights.map({ f =>
      termIds(f._2)
    }).toArray
  }

  def getTopDocsForDoc(doc: String): Array[String] = {
    val idWeights = topDocsForDoc(idDocs(doc))
    idWeights.map(f => {
      docIds(f._2)
    }).toArray
  }

  def prepareTermIndex(): Seq[(String, Array[String], Array[String])] = {
    idTerms.map(f => (
      f._1, getTopTermsForTerm(f._1), getTopDocsForTerm(f._1)
    )
    ).toSeq
  }

  def prepareDocIndex(): Seq[(String, Array[String], Array[String])] = {
    idDocs.map(f => (
      f._1, getTopTermsForDoc(f._1), getTopDocsForDoc(f._1)
    )
    ).toSeq
  }


  val prepareDocIndexMoreInfo = (wikiData: DataFrame) => {
    val filtered = wikiData.select("title","contributor", "timestamp").filter(row => idDocs.contains(row(0).asInstanceOf[String])).collect()
    filtered.map(row => {
      val title = row(0).asInstanceOf[String]
      val contributor = row(1).asInstanceOf[String]
      val timestamp = row(2).asInstanceOf[String]
      (title, contributor, timestamp, getTopTermsForDoc(title), getTopDocsForDoc(title))
    }).toSeq
  }
}
