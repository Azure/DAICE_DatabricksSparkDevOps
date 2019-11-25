package tests

import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.SharedSparkContext 
import com.holdenkarau.spark.testing.RDDComparisons
import org.apache.spark.rdd.RDD

import com.microsoft.spark.example.SparkSimpleApp

class AppTest extends FunSuite with SharedSparkContext with RDDComparisons {

  test("test RDDComparisons") {
    val inputRDD = sc.parallelize(Seq("a,b,c", "a", "d,e"))
    val expectedRDD:RDD[Integer] = sc.parallelize(Seq(3, 1, 2))
    val resultRDD = SparkSimpleApp.rdd_csv_col_count(inputRDD)

    assertRDDEqualsWithOrder(expectedRDD, resultRDD)
  }
}