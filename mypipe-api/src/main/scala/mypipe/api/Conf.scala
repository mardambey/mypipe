package mypipe.api

import java.io.{ File, PrintWriter }

import com.typesafe.config.ConfigFactory
import mypipe.mysql.BinaryLogFilePosition
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

object Conf {

  val log = LoggerFactory.getLogger(getClass)
  val conf = ConfigFactory.load()

  val DATADIR = conf.getString("mypipe.data-dir")
  val LOGDIR = conf.getString("mypipe.log-dir")

  val SHUTDOWN_FLUSH_WAIT_SECS = conf.getInt("mypipe.shutdown-wait-time-seconds")
  val GROUP_EVENTS_BY_TX = conf.getBoolean("mypipe.group-events-by-tx")
  val FLUSH_INTERVAL_SECS = conf.getInt("mypipe.flush-interval-seconds")

  val MYSQL_SERVER_ID_PREFIX = conf.getInt("mypipe.mysql-server-id-prefix")

  private val lastBinlogFilePos = scala.collection.concurrent.TrieMap[String, BinaryLogFilePosition]()

  try {
    new File(DATADIR).mkdirs()
    new File(LOGDIR).mkdirs()
  } catch {
    case e: Exception ⇒ println(s"Error while creating data and log dir ${DATADIR}, ${LOGDIR}: ${e.getMessage}")
  }

  def binlogGetStatusFilename(hostname: String, port: Int, pipe: String): String = {
    s"$DATADIR/$pipe-$hostname-$port.pos"
  }

  def binlogLoadFilePosition(hostname: String, port: Int, pipe: String): Option[BinaryLogFilePosition] = {
    try {

      val statusFile = binlogGetStatusFilename(hostname, port, pipe)
      val filePos = scala.io.Source.fromFile(statusFile).getLines().mkString.split(":")
      Some(BinaryLogFilePosition(filePos(0), filePos(1).toLong))

    } catch {
      case e: Exception ⇒ None
    }
  }

  def binlogSaveFilePosition(hostname: String, port: Int, filePos: BinaryLogFilePosition, pipe: String) {

    val key = binlogGetStatusFilename(hostname, port, pipe)

    if (!lastBinlogFilePos.getOrElse(key, "").equals(filePos)) {

      val fileName = binlogGetStatusFilename(hostname, port, pipe)
      val file = new File(fileName)
      val writer = new PrintWriter(file)

      log.info(s"Saving binlog position for pipe $pipe/$hostname:$port -> $filePos")
      writer.write(s"${filePos.filename}:${filePos.pos}")
      writer.close()

      lastBinlogFilePos(key) = filePos
    }
  }

  def loadClassesForKey[T](key: String): Map[String, Class[T]] = {
    val classes = Conf.conf.getObject(key).asScala
    classes.map(kv ⇒ {
      val subKey = kv._1
      val classConf = conf.getConfig(s"$key.$subKey")
      val clazz = classConf.getString("class")
      (subKey, Class.forName(clazz).asInstanceOf[Class[T]])
    }).toMap
  }
}
