package mypipe.snapshotter

import com.github.mauricio.async.db.{ Connection, QueryResult }

import scala.concurrent.{ ExecutionContext, Future }

object MySQLSnapshotter {

  val trxIsolationLevel = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;"
  val autoCommit = "SET autocommit=0;"
  val flushTables = "FLUSH TABLES;"
  val readLock = "FLUSH TABLES WITH READ LOCK;"
  val showMasterStatus = "SHOW MASTER STATUS;"
  val unlockTables = "UNLOCK TABLES;"
  val selectFrom = { dbTable: String ⇒ s"SELECT * FROM $dbTable;" }
  val commit = "COMMIT;"

  def snapshot(tables: Seq[String])(implicit c: Connection, ec: ExecutionContext): Future[Seq[(String, QueryResult)]] = {
    val tableQueries = tables map (t ⇒ t -> selectFrom(t))
    runQueries(queries(tableQueries))
  }

  private def queries(tableQueries: Seq[(String, String)]) = Seq(
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
          if (query._1.indexOf('.') > 0) {
            val dbName = query._1.splitAt(query._1.indexOf('.'))._1
            c.sendQuery(s"use $dbName;").flatMap { _ ⇒ c.sendQuery(query._2).map(r ⇒ queryResults :+ (query._1 -> r)) }
          } else {
            c.sendQuery(query._2).map(r ⇒ queryResults :+ (query._1 -> r))
          }
        } else {
          c.sendQuery(query._2).map(r ⇒ queryResults)
        }
      }
    }
  }
}
