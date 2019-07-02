import breeze.linalg.{DenseVector, DenseMatrix => BDenseMatrix, SparseVector => BSparseVector}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrices, Matrix, SingularValueDecomposition, Vectors, Vector => MLLibVector}

import scala.collection.Map

class LSAQueryEngine(
                      val svd: SingularValueDecomposition[RowMatrix, Matrix],
                      val termIds: Array[String],
                      val docIds: Map[Long, String],
                      val termIdfs: Array[Double]) {

  val VS: BDenseMatrix[Double] = multiplyByDiagonalMatrix(svd.V, svd.s)
  val normalizedVS: BDenseMatrix[Double] = rowsNormalized(VS)
  val US: RowMatrix = multiplyByDiagonalRowMatrix(svd.U, svd.s)
  val normalizedUS: RowMatrix = distributedRowsNormalized(US)
  val SV: BDenseMatrix[Double] = diagonalMultipliedByMatrix(svd.s, svd.V.transpose)

  val idTerms: Map[String, Int] = termIds.zipWithIndex.toMap
  val idDocs: Map[String, Long] = docIds.map(_.swap)

  /**
    * Finds the product of a dense matrix and a diagonal matrix represented by a vector.
    * Breeze doesn't support efficient diagonal representations, so multiply manually.
    */
  def multiplyByDiagonalMatrix(mat: Matrix, diag: MLLibVector): BDenseMatrix[Double] = {
    val sArr = diag.toArray
    new BDenseMatrix[Double](mat.numRows, mat.numCols, mat.toArray)
      .mapPairs { case ((r, c), v) => v * sArr(c) }
  }

  def diagonalMultipliedByMatrix(diag: MLLibVector, mat: Matrix): BDenseMatrix[Double] = {
    val sArr = diag.toArray
    new BDenseMatrix[Double](sArr.length, mat.numCols, mat.toArray)
      .mapPairs { case ((r, c), v) => v * sArr(r) }
  }

  /**
    * Finds the product of a distributed matrix and a diagonal matrix represented by a vector.
    */
  def multiplyByDiagonalRowMatrix(mat: RowMatrix, diag: MLLibVector): RowMatrix = {
    val sArr = diag.toArray
    new RowMatrix(mat.rows.map { vec =>
      val vecArr = vec.toArray
      val newArr = (0 until vec.size).toArray.map(i => vecArr(i) * sArr(i))
      Vectors.dense(newArr)
    })
  }

  /**
    * Returns a matrix where each row is divided by its length.
    */
  def rowsNormalized(mat: BDenseMatrix[Double]): BDenseMatrix[Double] = {
    val newMat = new BDenseMatrix[Double](mat.rows, mat.cols)
    for(r <- 0 until mat.rows) {
      val length = math.sqrt((0 until mat.cols).map(c => mat(r, c) * mat(r, c)).sum)
      (0 until mat.cols).foreach(c => newMat.update(r, c, mat(r, c) / length))
    }
    newMat
  }

  /**
    * Returns a distributed matrix where each row is divided by its length.
    */
  def distributedRowsNormalized(mat: RowMatrix): RowMatrix = {
    new RowMatrix(mat.rows.map { vec =>
      val array = vec.toArray
      val length = math.sqrt(array.map(x => x * x).sum)
      Vectors.dense(array.map(_ / length))
    })
  }

  /**
    * Finds docs relevant to a term. Returns the doc IDs and scores for the docs with the highest
    * relevance scores to the given term.
    */
  def topDocsForTerm(termId: Int): Seq[(Double, Long)] = {
    val rowArr = (0 until svd.V.numCols).map(i => svd.V(termId, i)).toArray
    val rowVec = Matrices.dense(rowArr.length, 1, rowArr)

    // Compute scores against every doc
    val docScores = US.multiply(rowVec)

    // Find the docs with the highest scores
    val allDocWeights = docScores.rows.map(_.toArray(0)).zipWithUniqueId
    allDocWeights.top(10)
  }


  def topTermsForDoc(docId: Long): Seq[(Double, Int)] = {
    val docRowArr = normalizedUS.rows.zipWithUniqueId.map(_.swap)
      .lookup(docId).head.toArray
    val docRowMat = BDenseMatrix.zeros[Double](docRowArr.length, 1)
    for(i <- Range(0,docRowArr.length-1))
      docRowMat(i, 0) = docRowArr(i)
    val docRowVec = docRowMat(::, 0)
    // Compute scores against every term
    val termScores = (VS * docRowVec).toArray.zipWithIndex

    termScores.sortBy(-_._1).take(10)
  }

  /**
    * Finds terms relevant to a term. Returns the term IDs and scores for the terms with the highest
    * relevance scores to the given term.
    */
  def topTermsForTerm(termId: Int): Seq[(Double, Int)] = {
    // Look up the row in VS corresponding to the given term ID.
    val rowVec = normalizedVS(termId, ::).t

    // Compute scores against every term
    val termScores = (normalizedVS * rowVec).toArray.zipWithIndex

    // Find the terms with the highest scores
    termScores.sortBy(-_._1).take(10)
  }


  def topDocsForDoc(docId: Long): Seq[(Double, Long)] = {
    val docRowArr = normalizedUS.rows.zipWithUniqueId.map(_.swap)
      .lookup(docId).head.toArray
    val docRowVec = Matrices.dense(docRowArr.length, 1, docRowArr)
    val docScores = normalizedUS.multiply(docRowVec)
    val allDocWeights = docScores.rows.map(_.toArray(0)).
      zipWithUniqueId()
    allDocWeights.filter(!_._1.isNaN).top(10)
  }




  def printTopDocsForDoc(doc: String): Unit = {
    val idWeights = topDocsForDoc(idDocs(doc))
    println(idWeights.map { case (score, id) =>
      (docIds(id), score)
    }.mkString(", "))
  }


  def printTopTermsForTerm(term: String): Unit = {
    val idWeights = topTermsForTerm(idTerms(term))
    println("Top terms for term: "+ term)
    println(idWeights.map { case (score, id) => (termIds(id), score) }.mkString(", "))
  }

  def getTopTermsForTerm(term: String): Array[String] = {
    val idWeights = topTermsForTerm(idTerms(term))
    val termsForTermString = idWeights.map { case (score, id) => termIds(id) }.mkString(", ")
    termsForTermString.split(", ")
  }

  def getTopDocsForTerm(term: String): Array[String] = {
    val idWeights = topDocsForTerm(idTerms(term))
    val docsForTermString = idWeights.map { case (score, id) => docIds(id) }.mkString(", ")
    docsForTermString.split(", ")
  }

  def getTopTermsForDoc(doc: String): Array[String] = {
    val idWeights = topTermsForDoc(idDocs(doc))
    val docsForDocString = idWeights.map { case (score, id) => termIds(id)}.mkString(", ")
    docsForDocString.split(", ")
  }

  def getTopDocsForDoc(doc: String): Array[String] = {
    val idWeights = topDocsForDoc(idDocs(doc))
    val docsForDocString = idWeights.map { case (score, id) => docIds(id)}.mkString(", ")
    docsForDocString.split(", ")
  }

  def printTopDocsForTerm(term: String): Unit = {
    val idWeights = topDocsForTerm(idTerms(term))
    println("Top documents for term: "+ term)
    println(idWeights.map { case (score, id) => (docIds(id), score) }.mkString(", "))
  }
}
