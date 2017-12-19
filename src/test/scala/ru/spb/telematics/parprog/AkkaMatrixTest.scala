package ru.spb.telematics.parprog

import org.scalatest.{FlatSpec, FunSuite}

import scala.util.Random

class AkkaMatrixTest extends FunSuite {

  test("Basic test of functionality") {
    val M = Vector[List[Int]](List[Int](3, 2, 1), List[Int](4, 3, 2), List[Int](5, 4, 3));
    val sM1 = AkkaMatrix.sortMatrix(M)
    val sM2 = M.toList.map(_.sorted)
    assert(sM1 == sM2)
  }

  test("Random permutations of the same matrix must give same result") {
    val rng = new Random(System.currentTimeMillis())
    val permutable = for (a <- 1 to 7) yield a
    val bigMatrix = permutable.permutations.toVector
    val sM1 = AkkaMatrix.sortMatrix(bigMatrix)
    val sM2 = bigMatrix.map(_.sorted)
    assert(sM1 == sM2)
  }

}
