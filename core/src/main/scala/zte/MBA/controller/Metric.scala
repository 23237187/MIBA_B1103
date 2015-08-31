package zte.MBA.controller

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.StatCounter
import zte.MBA.core.BaseEngine

import scala.Numeric.Implicits._
import scala.reflect.ClassTag


abstract class Metric[EI, Q, P, A, R](implicit rOrder: Ordering[R])
  extends Serializable {

  def header: String = this.getClass.getSimpleName

  def calculate(sc: SparkContext, evalDataSet: Seq[(EI, RDD[(Q, P, A)])]): R

  def compare(r0: R, r1: R): Int = rOrder.compare(r0, r1)
}

private [MBA] trait StatsMetricHelper[EI, Q, P, A] {
  def calculate(q: Q, p: P, a: A): Double

  def calculateStats(sc: SparkContext, evalDataSet: Seq[(EI, RDD[(Q, P, A)])])
  : StatCounter = {
    val doubleRDD = sc.union(
      evalDataSet.map { case (_, qpaRDD) =>
        qpaRDD.map{ case (q, p, a) => calculate(q, p, a)}
      }
    )

    doubleRDD.stats()
  }
}

private [MBA] trait StatsOptionMetricHelper[EI, Q, P, A] {
  def calculate(q: Q, p: P, a: A): Option[Double]

  def calculateStats(sc: SparkContext, evalDataSet: Seq[(EI, RDD[(Q, P, A)])])
  : StatCounter = {
    val doubleRDD = sc.union(
      evalDataSet.map { case (_, qpaRDD) =>
        qpaRDD.flatMap { case (q, p, a) => calculate(q, p, a) }
      }
    )

    doubleRDD.stats()
  }
}

abstract class AverageMetric[EI, Q, P, A]
  extends Metric[EI, Q, P, A, Double]
  with StatsMetricHelper[EI, Q, P, A]
  with QPAMetric[Q, P, A, Double] {

  def calculate(q: Q, p: P, a: A): Double

  def calculate(sc: SparkContext, evalDataSet: Seq[(EI, RDD[(Q, P, A)])]): Double = {
    calculateStats(sc, evalDataSet).mean
  }
}

abstract class OptionAverageMetric[EI, Q, P, A]
  extends Metric[EI, Q, P, A, Double]
  with StatsMetricHelper[EI, Q, P, A]
  with QPAMetric[Q, P, A, Option[Double]] {

  def calculate(q: Q, p: P, a: A): Option[Double]

  def calculate(sc: SparkContext, evalDataSet: Seq[(EI, RDD[(Q, P, A)])])
  : Double = {
    calculateStats(sc, evalDataSet).mean
  }
}

abstract class StdevMetric[EI, Q, P, A]
  extends Metric[EI, Q, P, A, Double]
  with StatsMetricHelper[EI, Q, P, A]
  with QPAMetric[Q, P, A, Double] {

  def calculate(q: Q, p: P, a: A): Double

  def calculate(sc: SparkContext, evalDataSet: Seq[(EI, RDD[(Q, P, A)])])
  : Double = {
    calculateStats(sc, evalDataSet).stdev
  }
}

abstract class OptionStdevMetric[EI, Q, P, A]
  extends Metric[EI, Q, P, A, Double]
  with StatsOptionMetricHelper[EI, Q, P, A]
  with QPAMetric[Q, P, A, Option[Double]] {

  def calculate(q: Q, p: P, a: A): Option[Double]

  def calculate(sc: SparkContext, evalDataSet: Seq[(EI, RDD[(Q, P, A)])])
  : Double = {
    calculateStats(sc, evalDataSet).stdev
  }
}

abstract class SumMetric[EI, Q, P, A, R: ClassTag](implicit num: Numeric[R])
  extends Metric[EI, Q, P, A, R]()(num)
  with QPAMetric[Q, P, A, R] {

  def calculate(q: Q, p: P, a: A): R

  def calculate(sc: SparkContext, evalDataSet: Seq[(EI, RDD[(Q, P, A)])])
  : R = {
    val union: RDD[R] = sc.union(
      evalDataSet.map { case (_, qpaRDD) =>
        qpaRDD.map { case (q, p, a) => calculate(q, p, a) }
      }
    )

    union.aggregate[R](num.zero)(_ + _ , _ + _)
  }
}

class ZeroMetric[EI, Q, P, A] extends Metric[EI, Q, P, A, Double]() {
  def calculate(sc: SparkContext, evalDataSet: Seq[(EI, RDD[(Q, P, A)])]): Double = 0.0
}

object ZeroMetric {
  def apply[EI, Q, P, A](engine: BaseEngine[EI, Q, P, A]): ZeroMetric[EI, Q, P, A] = {
    new ZeroMetric[EI, Q, P, A]()
  }
}

trait QPAMetric[Q, P, A, R] {
  def calculate(q: Q, p: P, a: A): R
}

