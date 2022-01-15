
/*
 * (C) Copyright IBM Corp. 2015 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 *  http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package src.main.scala;

import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.language.postfixOps
import scala.util.Random
import org.jblas.DoubleMatrix
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{ SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import java.io._

/**

 * Generate RDD(s) containing data for Matrix Multiplication.
 *
 * This method samples training entries according to the oversampling factor
 * 'trainSampFact', which is a multiplicative factor of the number of
 * degrees of freedom of the matrix: rank*(m+n-rank).
 *
 * It optionally samples entries for a testing matrix using
 * 'testSampFact', the percentage of the number of training entries
 * to use for testing.
 *
 * This method takes the following inputs:
 *   outputPath     (String) Directory to save output.
 *   m              (Int) Number of rows in data matrix.
 *   n              (Int) Number of columns in data matrix.
 *   rank           (Int) Underlying rank of data matrix.
 *   numPar         (Int) Number of partitions of input data file
 */

object MMDataGenerator {
  def main(args: Array[String]) {
    if (args.length < 1) {
      println("Usage: MMDataGenerator " +
        "<outputDir> [m] [n] [rank]  [numPar]")
      System.exit(1)
    }
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val outputPath: String = args(0)
    val m: Int = if (args.length > 1) args(1).toInt else 100
    val n: Int = if (args.length > 2) args(2).toInt else 100
    val rank: Int = if (args.length > 3) args(3).toInt else 10
    val trainSampFact: Double = if (args.length > 4) args(4).toDouble else 1.0
    val noise: Boolean = if (args.length > 5) args(5).toBoolean else false
    val sigma: Double = if (args.length > 6) args(6).toDouble else 0.1
    val test: Boolean = if (args.length > 7) args(7).toBoolean else false
    val testSampFact: Double = if (args.length > 8) args(8).toDouble else 0.1
    val defPar = if (System.getProperty("spark.default.parallelism") == null) 2 else System.getProperty("spark.default.parallelism").toInt
    val numPar: Int = if (args.length > 9) args(9).toInt else defPar

    val conf = new SparkConf().setAppName("MMDataGenerator")
    val sc = new SparkContext(conf)
	 
    val A = DoubleMatrix.randn(m, rank)
    val B = DoubleMatrix.randn(rank, n)
    val z = 1 / scala.math.sqrt(scala.math.sqrt(rank))
    A.mmuli(z)
    B.mmuli(z)
    val fullData = A.mmul(B)

    val mn = m * n

    val my_rdd=sc.makeRDD(1 to mn, numPar)
    //my_rdd.persist(StorageLevel.MEMORY_AND_DISK)
    val trainData: RDD[(Int, Int, Double)] = my_rdd
        .map(x => (fullData.indexRows(x.toInt - 1), fullData.indexColumns(x.toInt - 1), fullData.get(x.toInt - 1)))

    trainData.map(x => x._1 + "," + x._2 + "," + x._3).saveAsTextFile(outputPath)

    sc.stop()

  }
}
