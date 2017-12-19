package ru.spb.telematics.parprog

import java.util.concurrent.Executors

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import ru.spb.telematics.parprog.SortVectorActor.WhatToSort

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

object SortVectorActor {
  final case class WhatToSort(array: Seq[Int], row: Int)
}

class SortVectorActor extends Actor {
  import context.dispatcher
  def receive = {
    case wts: WhatToSort =>
      println("that was expected")
      sender ! wts.array.sorted
    case obj =>
      println("that was unexpected")
  }
}

// main class
object AkkaMatrix extends App {

  def sortMatrix(matrix: Seq[Seq[Int]]): Seq[Seq[Int]]  = {

    val es = Executors.newSingleThreadExecutor()
    implicit val ec = ExecutionContext.fromExecutorService(es)
    implicit val timeout = Timeout(5 seconds)

    val system: ActorSystem = ActorSystem("parallelMatrixSolver")

    val n = matrix.length
    val futures = for (a <- 0 until n) yield {
      // создаём актора, который будет сортировать a-тую строчку матрицы
      val sorter: ActorRef =
        system.actorOf(Props[SortVectorActor], "VectorActor" + a) // создаём актора
      val future: Future[Seq[Int]] = ask(sorter, WhatToSort(matrix(a), a)).mapTo[Seq[Int]] // даём задание
      future // собираем ссылки на будущие результаты Future
    }

    // барьер синхронизации -- общий сбор результатов в порядке следования строчек
    val sortedMatrix = for (a <- futures) yield {
      Await.result(a, 5 second)
    }

    ec.shutdown()
    sortedMatrix.toVector
  }


  val matrix = Vector[List[Int]](List[Int](3, 2, 1), List[Int](4, 3, 2), List[Int](5, 4, 3)) // входная матрица
  val result = sortMatrix(matrix)
  //print(result)
  System.exit(0)
}
