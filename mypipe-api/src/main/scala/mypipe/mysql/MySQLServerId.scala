package mypipe.mysql

import akka.util.Timeout
import akka.actor._
import akka.actor.ActorDSL._
import akka.pattern.ask
import mypipe.api.Conf
import scala.concurrent.Await
import scala.concurrent.duration._

object MySQLServerId {

  implicit val system = ActorSystem("mypipe")
  implicit val ec = system.dispatcher
  implicit val timeout = Timeout(1 second)

  case object Next

  val a = actor(new Act {
    var id = Conf.MYSQL_SERVER_ID_PREFIX
    become {
      case Next ⇒ {
        id += 1
        sender ! id
      }
    }
  })

  def next: Int = {

    val n = ask(a, Next)
    Await.result(n, 1 second).asInstanceOf[Int]
  }
}