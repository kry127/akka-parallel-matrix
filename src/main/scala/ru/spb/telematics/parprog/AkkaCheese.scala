package ru.spb.telematics.parprog

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import ru.spb.telematics.parprog.Buyer.{askCheese, cheese}
import ru.spb.telematics.parprog.BuyerQueue.{GotCheese, SendOneBuyer, StartGeneration}
import ru.spb.telematics.parprog.CheeseDispenser.getCheese
import ru.spb.telematics.parprog.Printer.BuyerInfo

import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.Random

/**
  * Для реализации "терпеливых" и "нетерпеливых" покупателей сыра воспользуемся приоритетами.
  * Правила построения очереди:
  *  -Терпеливым пользователям выдаётся приоритет 0
  *  -Нетерпеливым пользователям выдаётся приоритет 1-3
  *  -С течением времени приоритет увеличивается
  *  -Сыр выдаётся покупателю с максимальным приоритетом
  */
object AkkaCheese extends App {
  val queueSize = 10; // определяем размер очереди покупателей
  val system: ActorSystem = ActorSystem("CheeseWorld") // создаём акторную систему

  val printer = system.actorOf(Props[Printer], "printer") // актор печатающий информацию о покупке сыра
  val dispenser = system.actorOf(Props(new CheeseDispenser(system)), "dispenser") // актор сырный отдел
  val queue = system.actorOf(Props(new BuyerQueue(system, printer, dispenser, queueSize)), "queue") // актор очередь

  queue ! StartGeneration // запуск процесса заполнения очереди
  queue ! SendOneBuyer(1) // отправляем одного покупателя за сыром (пул из одного покупателя, первая касса)
  queue ! SendOneBuyer(2) // отправляем одного покупателя за сыром (пул из одного покупателя, вторая касса)

}

// объект-компаньон с сообщениями для актора "Queue" (очередь)
object BuyerQueue {
  case object StartGeneration
  case class GotCheese(cashdesk: Int) // выдан сыр покупателю на кассе с номером cashdesk
  case class SendOneBuyer(cashdesk: Int) // отправить следующего покупателя на кассу с номером cashdesk

  val honestThreshold = 2; // минимальное количество честных покупателей
  val impudentThreshold = 2; // минимальное количество нечестных покупателей
}

class BuyerQueue(system: ActorSystem, printer: ActorRef, cheeseDispenser: ActorRef, capacity: Int) extends Actor {
  /**
    * Процедура, отвечающая за создание терпеливых и нетерпеливых покупателей
    * Следит за тем, чтобы было как минимум два терпеливых и два нетерпеливых
    */
  implicit object Ord extends Ordering[(Int, Boolean, ActorRef)] {
    def compare(x: (Int, Boolean, ActorRef), y :(Int, Boolean, ActorRef)): Int = y._1.compare(x._1)
  }
  val buyers = new mutable.PriorityQueue[(Int, Boolean, ActorRef)]() // Создаём очередь с приоритетами. Неявная сортировка указана выше
  var time: Int = 0 // каждый новый вызов generate увеличивает генерируемое время на единицу


  def generate() : Unit = {
    time = time + 1
    val constTime = time // чтобы не было фейлов, делаем время константным
    def addPatient() = { // генерация честного покупателя
      val bRef: ActorRef = system.actorOf(Props(new Buyer(self, printer, cheeseDispenser,
        constTime, false)), "nextBuyer_" + constTime)
      buyers.enqueue((constTime, false, bRef))
    }
    def addImpudent() = { // генерация нетерпеливого покупателя
      val T = constTime - 1 - Random.nextInt(capacity / 2)
      val bRef: ActorRef = system.actorOf(Props(new Buyer(self, printer, cheeseDispenser,
        T, true)), "nextBuyer_" + constTime)
      buyers.enqueue((T, true, bRef))
    }
    if (buyers.map(_._2).count(a=> !a) < BuyerQueue.honestThreshold) {
      addPatient() // сначала генерим честных покупателей
      generate() // генерим, может честных не хватает ещё
    } else if (buyers.map(_._2).count(a=>a) < BuyerQueue.impudentThreshold) {
      addImpudent() // только потом уже генерим нетерпеливых покупателей
      generate() // генерим, может нечестных всё ещё тоже не хватает
    } else if (buyers.count(_=>true) < capacity) { // если достаточно честных и нечестных, генерим дополнительных
      if (Random.nextInt(2) == 0) addPatient() // причём генерим случайно честных или нечестных
      else addImpudent()
      generate()
    }
  }
  def sendOneBuyer(cashdesk: Int) : Unit = {
    val buyer = buyers.dequeue() // берём самого выгодного покупателя
    buyer._3 ! askCheese(cashdesk) // просим актора "сходить за сыром"
    generate() // после чего пополняем очередь новым борцом за сыр, и ждём, пока получение сыра завершится
  }
  override def receive = {
    case StartGeneration => generate()
    case SendOneBuyer(cashdesk) => sendOneBuyer(cashdesk)
    case GotCheese(cashdesk) => sendOneBuyer(cashdesk) // посылаем покупателей по одному в порядке очереди к кассе #cashdesk
  }
}

/**
  * Актор-покупатель товаров
  * time -- дискретное время прихода покупателя в очередь
  * меньше время -- больше приоритет
  * Наглецы занижают своё истинное время создания, тем самым ломятся вперёд и обслуживаются раньше
  */
object Buyer {
  case class cheese(cashdesk: Int) // получение сыра на кассе с номером cashdesk от сырного отдела
  case class askCheese(cashdesk: Int) // разрешение покупателю на доступ к кассе cashdesk у cheeseDispenser
}
class Buyer(val queue: ActorRef, val printer: ActorRef, val cheeseDispenser: ActorRef,
            val time: Int, val impudent: Boolean) extends Actor {
  def receive = {
    case Buyer.cheese(cashdesk) => queue ! GotCheese(cashdesk)
      printer ! BuyerInfo(this, cashdesk)
    case Buyer.askCheese(cashdesk) => cheeseDispenser ! getCheese(self, cashdesk)
  }
}

/**
  * Актор, выдающий сыр (сырный отдел)
  * Обслуживает акторов-покупателей Buyer
  */
object CheeseDispenser {
  case class getCheese(buyer: ActorRef, cashdesk: Int) // запрос от покупателя на получение сыра (запуск нити обработки)
  val averageDilation = 1500 // среднее время ожидание сыра в миллисекундах
  val interval = 1001 // длинна интервала равномерного распределения ожидания сыра
}
class CheeseDispenser(system: ActorSystem) extends Actor {
  import CheeseDispenser._
  import system.dispatcher
  override def receive = {
    case getCheese(buyer, cashdesk) =>
      // с некоторой задержкой отправляем сообщение обратно покупателю с сыром
      system.scheduler.scheduleOnce(averageDilation + Random.nextInt(interval) - interval/2 millisecond) {
        buyer ! cheese(cashdesk)
      }
  }
}

/**
  * Актор для записи в лог событий выдачи сыра. Можно настроить на логирование других действий
  */
object Printer {
  case class BuyerInfo(buyer: Buyer, cashdesk: Int)
}
class Printer extends Actor with ActorLogging {
  def receive = {
    case BuyerInfo(buyer, cashdesk) =>
      val T = buyer.time
      val I = buyer.impudent
      //log.info(s"Got cheese (from ${sender()}): time=$T, impudance=$I")
      log.info(s"Got cheese (from ${sender().path.name}): time=$T, impudance=$I, cashdesk=$cashdesk")
  }
}