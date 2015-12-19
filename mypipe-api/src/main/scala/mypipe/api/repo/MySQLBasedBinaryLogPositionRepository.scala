package mypipe.api.repo

import scala.concurrent.duration._
import scala.concurrent.Await
import com.github.mauricio.async.db.mysql.MySQLConnection
import com.github.mauricio.async.db.{ Connection, Configuration }
import com.typesafe.config.Config
import mypipe.api.HostPortUserPass
import mypipe.api.consumer.BinaryLogConsumer
import mypipe.mysql.BinaryLogFilePosition
import org.slf4j.LoggerFactory

class MySQLBasedBinaryLogPositionRepository(id: String, source: String, database: String, table: String) extends BinaryLogPositionRepository {

  protected val log = LoggerFactory.getLogger(getClass)

  protected var lastBinlogFilePos: Option[BinaryLogFilePosition] = None

  protected val connectionInfo = HostPortUserPass(source)
  protected val configuration = new Configuration(connectionInfo.user, connectionInfo.host, connectionInfo.port, Some(connectionInfo.password), Some(database))
  protected val connection: Connection = new MySQLConnection(configuration)

  connection.connect
  while (!connection.isConnected) {
    Thread.sleep(10)
  }

  createTable()

  private def createTable(): Unit = {
    try {
      val createF = connection.sendQuery(s"CREATE TABLE IF NOT EXISTS $table (id varchar(128), filename varchar(256), position integer(11), primary key (id)) engine InnoDB;")
      Await.result(createF, 5.seconds)
    } catch {
      case e: Exception ⇒
        log.error(s"Error while creating table at ${connectionInfo.user}@${connectionInfo.host}:${connectionInfo.port}/${database}/${table} id: $id, ${e.getMessage}: ${e.getStackTraceString}")
    }
  }

  override def saveBinaryLogPosition(consumer: BinaryLogConsumer[_]): Unit = {
    log.info(s"Saving binlog position for $id/${consumer.id} -> ${consumer.getBinaryLogPosition}")
    val consumerId = consumer.id
    val filePos = consumer.getBinaryLogPosition.getOrElse({
      log.warn(s"Tried saving non-existent binary log position for consumer $consumerId, saving ${BinaryLogFilePosition.current} instead.")
      BinaryLogFilePosition.current
    })

    try {
      val insertF = connection.sendQuery(s"INSERT INTO $table (id, filename, position) VALUES ('$id', '${filePos.filename}', ${filePos.pos}) ON DUPLICATE KEY UPDATE filename=VALUES(filename), position=VALUES(position)")

      Await.result(insertF, 5.seconds)
    } catch {
      case e: Exception ⇒
        log.error(s"Error while saving binary log position for consumer $consumerId, ${e.getMessage}: ${e.getStackTraceString}")
    }
  }

  override def loadBinaryLogPosition(consumer: BinaryLogConsumer[_]): Option[BinaryLogFilePosition] = {
    try {

      val filePosF = connection.sendQuery(s"SELECT filename, position FROM $table WHERE id = '$id'")

      Await.result(filePosF, 5.seconds).rows flatMap { rows ⇒
        rows.headOption map { row ⇒
          BinaryLogFilePosition(row("filename").asInstanceOf[String], row("position").asInstanceOf[Long])
        }
      }

    } catch {
      case e: Exception ⇒ None
    }
  }
}

class ConfigurableMySQLBasedBinaryLogPositionRepository(override val config: Config)
  extends MySQLBasedBinaryLogPositionRepository(
    config.getString("id"),
    config.getString("source"),
    config.getString("database"),
    config.getString("table"))
  with ConfigurableBinaryLogPositionRepository

