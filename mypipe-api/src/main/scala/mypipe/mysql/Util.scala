package mypipe.mysql

import com.github.mauricio.async.db.{ Connection, QueryResult }
import mypipe.api.data.{ ColumnMetadata, ColumnType, PrimaryKey }
import org.slf4j.LoggerFactory

import scala.concurrent.{ ExecutionContext, Future }

object Util {

  protected val log = LoggerFactory.getLogger(getClass)

  def getTableColumnsQuery(table: String, db: String) =
    s"""select COLUMN_NAME, DATA_TYPE, COLUMN_KEY from COLUMNS where TABLE_SCHEMA="$db" and TABLE_NAME = "$table" order by ORDINAL_POSITION"""
  def getPrimaryKeyQuery(table: String, db: String) =
    s"""select COLUMN_NAME from KEY_COLUMN_USAGE where TABLE_SCHEMA='$db' and TABLE_NAME='$table' and CONSTRAINT_NAME='PRIMARY' order by ORDINAL_POSITION"""

  def getTableColumns(db: String, table: String, dbConn: Connection)(implicit ec: ExecutionContext): Future[List[(String, String, Boolean)]] = {
    val futureCols: Future[QueryResult] = dbConn.sendQuery(getTableColumnsQuery(table, db))

    val mapCols: Future[List[(String, String, Boolean)]] = futureCols.map(queryResult ⇒ queryResult.rows match {
      case Some(resultSet) ⇒
        resultSet.map(row ⇒ {
          (row(0).asInstanceOf[String], row(1).asInstanceOf[String], row(2).equals("PRI"))
        }).toList

      case None ⇒
        List.empty[(String, String, Boolean)]
    })
    mapCols
  }

  def getPrimaryKey(db: String, table: String, dbConn: Connection)(implicit ec: ExecutionContext): Future[Option[List[String]]] = {
    val futurePkey: Future[QueryResult] = dbConn.sendQuery(getPrimaryKeyQuery(table, db))

    val pKey: Future[Option[List[String]]] = futurePkey.map(queryResult ⇒ queryResult.rows match {

      case Some(resultSet) if resultSet.nonEmpty ⇒
        Some(resultSet.map(row ⇒ row(0).asInstanceOf[String]).toList)

      case Some(resultSet) ⇒
        log.debug(s"No primary key determined for $db:$table")
        None

      case None ⇒
        log.error(s"Failed to determine primary key for $db:$table")
        None
    })

    pKey
  }

  def getPrimaryKeyFromColumns(primaryKeyColumnNames: List[String], columns: List[ColumnMetadata]): PrimaryKey = {
    val primaryKeyCols: List[ColumnMetadata] = primaryKeyColumnNames.map(colName ⇒ {
      columns.find(_.name.equals(colName)).get
    })

    PrimaryKey(primaryKeyCols)
  }

  def createColumns(columns: List[(String, String, Boolean)]): List[ColumnMetadata] = {
    try {
      // TODO: if the table definition changes we'll overflow due to the following being larger than colTypes
      var cur = 0

      val cols = columns.map(c ⇒ {
        val colName = c._1
        val colTypeStr = c._2
        val isPrimaryKey = c._3
        val colType = ColumnType.typeByString(colTypeStr).getOrElse(ColumnType.UNKNOWN)
        cur += 1
        ColumnMetadata(colName, colType, isPrimaryKey)
      })

      cols

    } catch {
      case e: Exception ⇒
        log.error(s"Failed to determine column names: $columns\n${e.getMessage} -> ${e.getStackTraceString}")
        List.empty[ColumnMetadata]
    }
  }
}
