package mypipe.snapshotter

import akka.util.Timeout
import com.github.mauricio.async.db.Connection
import mypipe.{ Queries, ActorSystemSpec, DatabaseSpec, UnitSpec }
import org.scalatest.BeforeAndAfterAll
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.Await

class SnapshotterSpec extends UnitSpec with DatabaseSpec with ActorSystemSpec with BeforeAndAfterAll {

  val log = LoggerFactory.getLogger(getClass)

  implicit val timeout = Timeout(1.second)

  "Snapshotter" should "be able to fetch table information" in {

    implicit lazy val c: Connection = db.connection

    val future = {
      // make inserts
      Await.result(db.connection.sendQuery(Queries.INSERT.statement(id = "123")), 2000.millis)
      Await.result(db.connection.sendQuery(Queries.INSERT.statement(id = "124")), 2000.millis)

      val tables = Seq("mypipe.user")

      val colData = MySQLSnapshotter.snapshot(tables) map { results ⇒
        results.map { result ⇒
          val colData = result._2.rows.map(identity) map { rows ⇒
            val colCount = rows.columnNames.length
            rows.map { row ⇒
              (0 until colCount) map { i ⇒
                row(i)
              }
            }
          }

          result._1 -> colData.getOrElse(Seq.empty)
        }
      }

      val ret = colData
        .map { colData ⇒
          colData.map {
            case (dbAndTable, rows) ⇒
              if (dbAndTable.equals("showMasterStatus")) {
                log.info(s"found show master status data: length:${rows.length} data:${rows.map(_.toArray.map(c ⇒ c.getClass.getName + ":" + c.toString).mkString(",")).mkString("\n")}")
                rows.length == 1 &&
                  rows(0).length >= 2 // file and position at least
              } else if (dbAndTable.startsWith("mypipe.") && rows.length > 0) {
                log.info(s"found select data: length:${rows.length} data:${rows.map(_.toArray.map(c ⇒ c.getClass.getName + ":" + c.toString).mkString(",")).mkString("\n")}")
                rows.length == 2 &&
                  rows(0).toArray.deep.equals(Array(123, "username", "password", 0, "bio").deep) &&
                  rows(1).toArray.deep.equals(Array(124, "username", "password", 0, "bio").deep)
              } else {
                true
              }
          }
        }

      ret.map { results ⇒
        !results.contains(false)
      }
    }

    try {
      val ret = Await.result(future, 10.seconds)
      assert(ret)
    } catch {
      case e: Exception ⇒
        log.error(s"Caught exception: ${e.getMessage} at ${e.getStackTraceString}")
        assert(false)
    }
  }
}

