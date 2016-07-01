package mypipe.snapshotter

import com.github.mauricio.async.db.{Connection, QueryResult}
import mypipe.api.data.{ColumnMetadata, ColumnType}
import mypipe.mysql.BinaryLogFilePosition
import mypipe.mysql.Util
import mypipe.snapshotter.splitter.{InputSplit, IntegerSplitter}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

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

  val log = LoggerFactory.getLogger(getClass)

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

  def snapshot(db: String, table: String, splitByCol: Option[String], selectQuery: Option[String], eventHandler: Seq[Option[SnapshotterEvent]] ⇒ Unit)(implicit c: Connection, ec: ExecutionContext) = {

    // find primary key if it exists
    val primaryKeyOptF = for (
      _ ← c.sendQuery(s"use information_schema");
      columnsMap ← Util.getTableColumns(db, table, c);
      primaryKeyColumnNamesOpt ← Util.getPrimaryKey(db, table, c)
    ) yield {
      val columns = Util.createColumns(columnsMap)
      log.info(s"Found primary key: $primaryKeyColumnNamesOpt")
      log.info(s"Found columns: $columns")
      primaryKeyColumnNamesOpt map {
        Util.getPrimaryKeyFromColumns(_, columns)
      }
    }

    val splitQueriesListF: Future[List[(String, String)]] = primaryKeyOptF flatMap {
      case Some(primaryKey) if primaryKey.columns.size == 1 ⇒

        // get splits
        log.info(s"Trying to use primary key $primaryKey as split-by column.")
        val splitsF = getSplits(db, table, primaryKey.columns.head)

        // TODO: handle no splits returned
        // create a series of:
        // $table -> "use $db"
        // $table -> "select * from $table ..."
        val splitQueriesF = splitsF map { splits ⇒
          splits.map { split ⇒
            (s"$db.$table" → s"use $db",
              s"$db.$table" → s"""SELECT * FROM $table WHERE ${split.lowClausePrefix} and ${split.upperClausePrefix}""")
          }.foldLeft(List.empty[(String, String)]) { (acc: List[(String, String)], v: ((String, String), (String, String))) ⇒
            acc ++ List(v._1, v._2)
          }
        }

        splitQueriesF

      case Some(primaryKey) if primaryKey.columns.size > 1 ⇒
        log.error(s"Could not use multi-column primary key as a split-by column, aborting: $primaryKey")
        Future.successful(List.empty[(String, String)])
      case None ⇒
        log.error("Could not find a suitable primary key to use as a split-by column, aborting.")
        Future.successful(List.empty[(String, String)])
    }

    splitQueriesListF map { splitQueriesList ⇒
      val queries = queriesWithoutTxn(splitQueriesList)
      _executeQueries(queries, eventHandler)
    }
  }

  private def _executeQueries(queries: Seq[(String, String)], eventHandler: Seq[Option[SnapshotterEvent]] ⇒ Unit)(implicit c: Connection, ec: ExecutionContext): Future[Unit] = {

    val queryKey = queries.head._1
    val queryVal = queries.head._2

    log.info(queryVal)
    c.sendQuery(queryVal).map { queryResult ⇒

      if (!queryKey.isEmpty) {
        val events = snapshotToEvents(Seq(queryKey → queryResult))
        eventHandler(events)
      }

      queries.tail

    } flatMap {
      case queries if queries.nonEmpty ⇒
        _executeQueries(queries, eventHandler)
      case _ ⇒
        Future.successful { Unit }
    }
  }

  def getSplits(db: String, table: String, splitByCol: ColumnMetadata)(implicit c: Connection, ec: ExecutionContext): Future[List[InputSplit]] = {
    splitByCol.colType match {

      case ColumnType.INT24 ⇒

        val boundingQuery = s"""SELECT MIN(${splitByCol.name}), MAX(${splitByCol.name}) FROM $db.$table""" // TODO: append user provided WHERE clause if any

        log.info(s"use $db")
        c.sendQuery(s"use $db") flatMap { _ ⇒
          log.info(s"$boundingQuery")
          val boundingValuesF = c.sendQuery(boundingQuery)
          boundingValuesF map { boundingValues ⇒
            val r = boundingValues.rows flatMap { rows ⇒
              rows.headOption map { row ⇒
                val lowerBound = row(0).asInstanceOf[Int]
                val upperBound = row(1).asInstanceOf[Int]

                IntegerSplitter.split(splitByCol, lowerBound, upperBound)
              }
            }

            r.getOrElse(List.empty)
          }
        }

      case x ⇒
        log.error("split-by column of type $x is not supported, aborting.")
        Future.successful(List.empty)
    }

  }

  // TODO: we can still modify and use this to run the queries and spit out events
  def snapshot(tables: Seq[String], withTransaction: Boolean = true)(implicit c: Connection, ec: ExecutionContext): Future[Seq[(String, QueryResult)]] = {
    val tableQueries = tables
      .map({ t ⇒ (t → useDatabase(t), t → selectFrom(t)) })
      .foldLeft(List.empty[(String, String)]) { (acc: List[(String, String)], v: ((String, String), (String, String))) ⇒
        acc ++ List(v._1, v._2)
      }

    val queries =
      (if (withTransaction) queriesWithTxn _
      else queriesWithoutTxn _) apply tableQueries

    runQueries(queries)
  }

  def snapshotToEvents(snapshot: Future[Seq[(String, QueryResult)]])(implicit ec: ExecutionContext): Future[Seq[Option[SnapshotterEvent]]] = snapshot map { snapshotToEvents }

  def snapshotToEvents(results: Seq[(String, QueryResult)]): Seq[Option[SnapshotterEvent]] = {
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

  private def queriesWithoutTxn(tableQueries: Seq[(String, String)]) = Seq(
    "showMasterStatus" → showMasterStatus
  ) ++ tableQueries

  private def queriesWithTxn(tableQueries: Seq[(String, String)]) = Seq(
    "" → trxIsolationLevel,
    "" → autoCommit,
    "" → flushTables,
    "" → readLock,
    "showMasterStatus" → showMasterStatus,
    "" → unlockTables
  ) ++ tableQueries ++ Seq(
      "" → commit
    )

  private def runQueries(queries: Seq[(String, String)])(implicit c: Connection, ec: ExecutionContext): Future[Seq[(String, QueryResult)]] = {
    queries.foldLeft[Future[Seq[(String, QueryResult)]]](Future.successful(Seq.empty)) { (future, query) ⇒
      future flatMap { queryResults ⇒
        if (!query._1.isEmpty) {
          log.info(s"${query._2}")
          c.sendQuery(query._2).map(r ⇒ queryResults :+ (query._1 → r))
        } else {
          log.info(s"${query._2}")
          c.sendQuery(query._2).map(r ⇒ queryResults)
        }
      }
    }
  }
}
