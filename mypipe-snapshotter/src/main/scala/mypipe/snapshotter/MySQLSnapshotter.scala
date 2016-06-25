package mypipe.snapshotter

import com.github.mauricio.async.db.{ Connection, QueryResult }
import mypipe.api.data.{ ColumnMetadata, ColumnType }
import mypipe.mysql.BinaryLogFilePosition
import mypipe.mysql.Util
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ ExecutionContext, Future }

/** Interesting parameters:
 *  - boundary-query: used for creating splits
 *  - query: import the results of this query
 *  - split-by: column used to split work units
 *  - where: where clause used during import
 *
 *  By default we will use the query:
 *   select min(<split-by>), max(<split-by>) from <table name>
 *  to find out boundaries for creating splits.
 *
 *  In some cases this query is not the most optimal
 *  so you can specify any arbitrary query returning
 *  two numeric columns using boundary-query argument.
 *
 *  By default, the split-by column is the primary key.
 *
 *  1. Determine primary key and use as split-by column, or used given split-by column
 *  2. Based on type of split-by column, determine data ranges for the table
 *  3. For each data range, use built in select query or use provided query to fetch data
 *  4. For each row, turn it into a SelectEvent and push it into the SelectConsumer's pipe.
 */
object MySQLSnapshotter {

  val trxIsolationLevel = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"
  val autoCommit = "SET autocommit=0"
  val flushTables = "FLUSH TABLES"
  val readLock = "FLUSH TABLES WITH READ LOCK"
  val showMasterStatus = "SHOW MASTER STATUS"
  val unlockTables = "UNLOCK TABLES"
  val commit = "COMMIT"

  val selectFrom = { dbTable: String ⇒ s"SELECT * FROM $dbTable" }
  val useDatabase = { dbTable: String ⇒
    val dbName = dbTable.splitAt(dbTable.indexOf('.'))._1
    s"use $dbName"
  }

  def snapshot(db: String, table: String, splitByCol: Option[String], selectQuery: Option[String])(implicit c: Connection, ec: ExecutionContext) = {

    // get table columns and primary key
    val columnsF = Util.getTableColumns(db, table, c)
    val primaryKeyF = Util.getPrimaryKey(db, table, c)

    val seqF = Future.sequence(Seq(columnsF, primaryKeyF))

    // find primary key if it exists
    val primaryKeyOptF = seqF map { futures: Seq[_] ⇒
      val columnsMap = futures(0).asInstanceOf[List[(String, String, Boolean)]]
      val columns = Util.createColumns(columnsMap)
      val primaryKeyColumnNamesOpt = futures(1).asInstanceOf[Option[List[String]]]

      primaryKeyColumnNamesOpt map {
        Util.getPrimaryKeyFromColumns(_, columns)
      }
    }

    primaryKeyF

    ???
  }

  def getSplits(db: String, table: String, splitByCol: ColumnMetadata) = {
    splitByCol.colType match {

      case ColumnType.INT24 ⇒ //IntegerSplitter()
    }

    ???
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
