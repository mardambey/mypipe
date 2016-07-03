package mypipe.snapshotter

import akka.util.Timeout
import com.github.mauricio.async.db.Connection
import mypipe.{ActorSystemSpec, DatabaseSpec, Queries, UnitSpec}
import org.scalatest.BeforeAndAfterAll
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.Await

class SnapshotterSpec extends UnitSpec with DatabaseSpec with ActorSystemSpec with BeforeAndAfterAll {

  val log = LoggerFactory.getLogger(getClass)

  implicit val timeout = Timeout(1.second)

  "Snapshotter" should "be able to fetch table information" in {

    implicit lazy val c: Connection = db.connection
    val asserts = ListBuffer[Boolean]()

    val future = {
      // make inserts
      Await.result(db.connection.sendQuery(Queries.INSERT.statement(id = "123")), 2000.millis)
      Await.result(db.connection.sendQuery(Queries.INSERT.statement(id = "124")), 2000.millis)

      val tables = Seq("mypipe.user")

      MySQLSnapshotter.snapshot(
        db = tables.head.split("\\.").head,
        table = tables.head.split("\\.")(1),
        numSplits = 5,
        splitLimit = 100,
        splitByColumnName = None,
        selectQuery = None,
        whereClause = None,
        eventHandler = { results ⇒

          results.map {
            case result: SelectEvent ⇒
              val rows = result.rows
              log.info(s"found select data: length:${rows.length} data:${rows.map(_.toArray.map(c ⇒ c.getClass.getName + ":" + c.toString).mkString(",")).mkString("\n")}")
              asserts += rows.isEmpty /* figure out exactly why we're doing this */ || (rows.length == 1 && (
                rows.head.toArray.deep.equals(Array(123, "username", "password", 0, "bio").deep) ||
                rows.head.toArray.deep.equals(Array(124, "username", "password", 0, "bio").deep)
              ))

            case result: ShowMasterStatusEvent ⇒
              log.info(s"found show master status data: $result}")
              asserts += true

            case x ⇒
              log.error(s"Found unknown event, ignoring $x")
          }

          // always continue
          true
        }
      )
    }

    try {
      Await.result(future, 10.seconds)
      assert(!asserts.contains(false) && asserts.length == 3)
    } catch {
      case e: Exception ⇒
        log.error(s"Caught exception: ${e.getMessage} at ${e.getStackTraceString}")
        assert(false)
    }
  }
}

