package mypipe.snapshotter

import com.github.mauricio.async.db.{ Connection, QueryResult }
import mypipe.mysql.BinaryLogFilePosition

import scala.concurrent.{ ExecutionContext, Future }

object MySQLSnapshotter {

  val trxIsolationLevel = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;"
  val autoCommit = "SET autocommit=0;"
  val flushTables = "FLUSH TABLES;"
  val readLock = "FLUSH TABLES WITH READ LOCK;"
  val showMasterStatus = "SHOW MASTER STATUS;"
  val unlockTables = "UNLOCK TABLES;"
  val commit = "COMMIT;"

  val selectFrom = { dbTable: String ⇒ s"SELECT * FROM $dbTable;" }
  val useDatabase = { dbTable: String ⇒
    val dbName = dbTable.splitAt(dbTable.indexOf('.'))._1
    s"use $dbName;"
  }

  def snapshot(tables: Seq[String], withTransaction: Boolean = true)(implicit c: Connection, ec: ExecutionContext): Future[Seq[(String, QueryResult)]] = {
    val tableQueries = tables
      .map({ t ⇒ (t -> useDatabase(t), t -> selectFrom(t)) })
      .foldLeft(List.empty[(String, String)]) { (acc: List[(String, String)], v: ((String, String), (String, String))) ⇒
        acc ++ List(v._1, v._2)
      }

    val queries =
      (if (withTransaction) queriesWithTxn _
      else queriesWithoutTxn _) apply tableQueries

    runQueries(queries)
  }

  def snapshotToEvents(snapshot: Future[Seq[(String, QueryResult)]])(implicit ec: ExecutionContext): Future[Seq[Option[SnapshotterEvent]]] = snapshot map {
    results ⇒
      {
        results.map { result ⇒
          val colData = result._2.rows.map(identity) map { rows ⇒
            val colCount = rows.columnNames.length
            rows.map { row ⇒
              (0 until colCount) map { i ⇒
                row(i)
              }
            }
          }

          val firstDot = result._1.indexOf('.')
          if (firstDot == result._1.lastIndexOf('.') && firstDot > 0) {
            val Array(db, table) = result._1.split('.')
            Some(SelectEvent(db, table, colData.getOrElse(Seq.empty)))
          } else if (result._1.equals("showMasterStatus")) {
            colData.getOrElse(Seq.empty).headOption.map(c ⇒
              ShowMasterStatusEvent(BinaryLogFilePosition(c(0).asInstanceOf[String], c(1).asInstanceOf[Long])))
          } else {
            None
          }
        }
      }
  }

  private def queriesWithoutTxn(tableQueries: Seq[(String, String)]) = Seq(
    "showMasterStatus" -> showMasterStatus) ++ tableQueries

  private def queriesWithTxn(tableQueries: Seq[(String, String)]) = Seq(
    "" -> trxIsolationLevel,
    "" -> autoCommit,
    "" -> flushTables,
    "" -> readLock,
    "showMasterStatus" -> showMasterStatus,
    "" -> unlockTables) ++ tableQueries ++ Seq(
      "" -> commit)

  private def runQueries(queries: Seq[(String, String)])(implicit c: Connection, ec: ExecutionContext): Future[Seq[(String, QueryResult)]] = {
    queries.foldLeft[Future[Seq[(String, QueryResult)]]](Future.successful(Seq.empty)) { (future, query) ⇒
      future flatMap { queryResults ⇒
        if (!query._1.isEmpty) {
          c.sendQuery(query._2).map(r ⇒ queryResults :+ (query._1 -> r))
        } else {
          c.sendQuery(query._2).map(r ⇒ queryResults)
        }
      }
    }
  }
}
