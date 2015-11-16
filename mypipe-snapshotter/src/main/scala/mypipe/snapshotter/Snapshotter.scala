package mypipe.snapshotter

import scala.concurrent.duration._
import com.github.mauricio.async.db.{QueryResult, Connection}
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import scala.concurrent.{Future, Await}

object Snapshotter extends App {

  protected val log = LoggerFactory.getLogger(getClass)
  protected val conf = ConfigFactory.load()

  implicit val c: Connection = ???

  val tables = Seq("mypipe.user")

  val selects = snapshotToSelects(MySQLSnapshotter.snapshot(tables))

  selects

  sys.addShutdownHook({
    log.info("Shutting down...")
  })

  def snapshotToSelects(snapshot: Future[Seq[(String, QueryResult)]]): Future[Seq[SelectEvent]] = snapshot map {
    results ⇒ {
      results.map { result ⇒
        val colData = result._2.rows.map(identity) map { rows ⇒
          val colCount = rows.columnNames.length
          rows.map { row ⇒
            (0 until colCount) map { i ⇒
              row(i)
            }
          }
        }

        val (db, table) = result._1.split('.')
        SelectEvent(db, table, colData.getOrElse(Seq.empty))
      }
    }
  }

}

